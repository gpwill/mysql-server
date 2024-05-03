/* Copyright (c) 2006, 2023, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <ctype.h>
#include <fcntl.h>
#include <mysql/plugin.h>
#include <mysql_version.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "m_string.h"  // strlen
#include "my_dbug.h"
#include "my_dir.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "my_psi_config.h"
#include "my_sys.h"  // my_write, my_malloc
#include "my_thread.h"
#include "thr_lock.h"
#include "mysql/psi/mysql_memory.h"
#include "sql/sql_plugin.h"  // st_plugin_int
#include "sql/sql_class.h"
#include "sql/sql_base.h"
#include "sql/sql_table.h"
#include "sql/table.h"
#include "sql/key.h"
#include "sql/field.h"
#include "sql/log.h"
#include "sql/mdl.h"
#include "sql/transaction.h"
#include "sql/handler.h"
#include "sql/sql_lex.h"
#include "sql/protocol_classic.h"
#include "sql/dd_table_share.h"
#include "sql/dd/types/table.h"
#include "sql/dd/cache/dictionary_client.h"

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include "spectrum.grpc.pb.h"
#include "spectrum.h"

PSI_memory_key key_memory_mysql_heartbeat_context;

#ifdef HAVE_PSI_INTERFACE

static PSI_memory_info all_deamon_example_memory[] = {
    {&key_memory_mysql_heartbeat_context, "mysql_heartbeat_context", 0, 0,
     PSI_DOCUMENT_ME}};

static void init_deamon_example_psi_keys() {
  const char *category = "deamon_example";
  int count;

  count = static_cast<int>(array_elements(all_deamon_example_memory));
  mysql_memory_register(category, all_deamon_example_memory, count);
}
#endif /* HAVE_PSI_INTERFACE */

#define HEART_STRING_BUFFER 100

THD *handler_create_thd(const spectrum::Thread &spectrum_thread)
{
  THD *thd;
  MDL_key mdl_key;
  MDL_request_list mdl_requests;

  my_thread_init();

  thd = new (std::nothrow) THD;
  if (!thd) {
    return (NULL);
  }

  thd->get_protocol_classic()->init_net((Vio *)0);
  thd->set_new_thread_id();
  thd->thread_stack = reinterpret_cast<char *>(&thd);
  thd->store_globals();

  // Disable auto commit
  thd->variables.option_bits |= OPTION_NOT_AUTOCOMMIT;
  thd->variables.option_bits &= ~OPTION_AUTOCOMMIT;

  for (unsigned int i = 0; i < spectrum_thread.mdl_list().size(); i++) {
    spectrum::MDL mdl = spectrum_thread.mdl_list()[i];    
    
    MDL_request mdl_request;
    mdl_key.mdl_key_init(static_cast<MDL_key::enum_mdl_namespace>(mdl.namespace_()), mdl.schema().c_str(), mdl.table().c_str(), mdl.column().c_str());
    MDL_REQUEST_INIT_BY_KEY(&mdl_request, &mdl_key, static_cast<enum_mdl_type>(mdl.type()), MDL_TRANSACTION);
    mdl_requests.push_front(&mdl_request);
  }
  thd->mdl_context.acquire_locks(&mdl_requests, 10000);
  return (thd);
}

TABLE *handler_open_table(
    THD *thd,           /*!< in: THD* */
    const char *db_name,    /*!< in: NUL terminated database name */
    const char *table_name) /*!< in: NUL terminated table name */
{
  Open_table_context table_ctx(thd, 0);
  thr_lock_type lock_mode = TL_WRITE;

  Table_ref tables(db_name, strlen(db_name), table_name, strlen(table_name),
                   table_name, lock_mode);
  if (!open_table(thd, &tables, &table_ctx)) {
    TABLE *table = tables.table;
    table->use_all_columns();
    sql_print_information("handler_open_table[%s:%s]: success", db_name, table_name);
    return table;
  }
  return NULL;
}

class StorageNodeImpl final : public spectrum::StorageNode::Service {
  public:
    ::grpc::Status CreateTable(::grpc::ServerContext* context, const ::spectrum::CreateTableRequest* request, ::spectrum::CreateTableResponse* response) {
      THD *thd;
      int error;
      const dd::Table *table_def = nullptr;
      dd::Table *table_def_clone;
      TABLE_SHARE table_share;
      char table_filepath[FN_REFLEN + 1];
      HA_CREATE_INFO create_info;
      MDL_request_list mdl_requests;
      const char* db_name = request->database().c_str();
      const char* table_name = request->table().c_str();

      thd = handler_create_thd(request->thread());
      // Request metadata lock on schema and table
      //MDL_request schema_mdl_request;
      //MDL_REQUEST_INIT(&schema_mdl_request, MDL_key::SCHEMA, db_name, "", MDL_EXCLUSIVE, MDL_TRANSACTION);
      //mdl_requests.push_front(&schema_mdl_request);
      //MDL_request table_mdl_request;
      //MDL_REQUEST_INIT(&table_mdl_request, MDL_key::TABLE, db_name, table_name, MDL_EXCLUSIVE, MDL_TRANSACTION);
      //mdl_requests.push_front(&table_mdl_request);
      //thd->mdl_context.acquire_locks(&mdl_requests, 10000);
    
      // Retrive table definition
      dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
      thd->dd_client()->acquire(db_name, table_name, &table_def);
      if (table_def == nullptr) {
        sql_print_error("CreateTable[%s:%s]: can not find table definition", db_name, table_name);
        goto end;
      }
      
      table_def_clone = table_def->clone();
      build_table_filename(table_filepath, sizeof(table_filepath) - 1, db_name, table_name, "", 0);      
      error = ha_create_table(thd, table_filepath, db_name, table_name, &create_info, true, false, table_def_clone);
      if (error) {
        sql_print_error("CreateTable[%s:%s]: can not create table in ha, error=%d", db_name, table_name, error);
        goto end;
      }

    end:
      thd->lex->sql_command = SQLCOM_CREATE_TABLE;
      handlerton *db_type = get_viable_handlerton_for_create(thd, table_name, create_info);
      thd->m_transactional_ddl.init(db_name, table_name, db_type);
      trans_commit_stmt(thd, false);
      trans_commit(thd, false);
      thd->mdl_context.release_transactional_locks();
      return grpc::Status::OK; 
    }

    ::grpc::Status WriteRow(::grpc::ServerContext* context, const ::spectrum::WriteRowRequest* request, ::spectrum::WriteRowResponse* response) {
      THD *thd;
      TABLE *table;

      thd = handler_create_thd(request->thread());
      
      table = handler_open_table(thd, request->database().c_str(), request->table().c_str());
      empty_record(table);
      memset(table->record[0], 0, table->s->null_bytes);
      table->autoinc_field_has_explicit_non_null_value = request->autoinc_field_has_explicit_non_null_value();

      ::spectrum::Row spectrum_row = request->row();
      for (unsigned int i = 0; i < spectrum_row.fields().size(); i++) {
        Field *field = table->field[i];
        ::spectrum::Field spectrum_field = spectrum_row.fields()[i];
        if (!spectrum_field.is_null()) {
          std::string value = spectrum_field.value();
          field->store(value.c_str(), value.length(), &my_charset_bin);
        } else {
          field->set_null();
        }
      }

      // For autoincr field to work
      table->next_number_field = table->found_next_number_field;
      table->reginfo.lock_type = thr_lock_type::TL_WRITE;

      thd->lock = mysql_lock_tables(thd, &table, 1, 0);
      
      table->file->ha_write_row(table->record[0]);
      response->set_insert_id(table->file->insert_id_for_cur_row);

      table->file->ha_release_auto_increment();

      mysql_unlock_tables(thd, thd->lock);

      trans_commit_stmt(thd, false);
      trans_commit(thd, false);
      return grpc::Status::OK; 
    }

    ::grpc::Status UpdateRow(::grpc::ServerContext* context, const ::spectrum::WriteRowRequest* request, ::spectrum::WriteRowResponse* response) {
      return WriteRow(context, request, response);
    }

    ::grpc::Status CreateReplica(::grpc::ServerContext* context, const ::spectrum::CreateReplicaRequestMessage* request, ::spectrum::CreateReplicaResponseMessage* response) {
      return grpc::Status::OK;
    }

    ::grpc::Status UpdateEpoch(::grpc::ServerContext* context, const ::spectrum::UpdateEpochRequestMessage* request, ::spectrum::UpdateEpochResponseMessage* response) {
      return grpc::Status::OK;
    }

    ::grpc::Status Prepare(::grpc::ServerContext* context, const ::spectrum::PrepareRequestMessage* request, ::spectrum::PrepareResponseMessage* response) {
      return grpc::Status::OK;
    }

    ::grpc::Status Accept(::grpc::ServerContext* context, const ::spectrum::AcceptRequestMessage* request, ::spectrum::AcceptResponseMessage* response) {
      return grpc::Status::OK;
    }
    
    ::grpc::Status Abort(::grpc::ServerContext* context, const ::spectrum::AbortRequestMessage* request, ::spectrum::AbortResponseMessage* response) {
      return grpc::Status::OK;
    }
};

struct mysql_heartbeat_context {
  my_thread_handle heartbeat_thread;
  File heartbeat_file;

  std::unique_ptr<grpc::Server> server;
};

static void *mysql_heartbeat(void *p) {
  DBUG_TRACE;
  struct mysql_heartbeat_context *con = (struct mysql_heartbeat_context *)p;
  char buffer[HEART_STRING_BUFFER];
  time_t result;
  struct tm tm_tmp;

  while (true) {
    sleep(5);

    result = time(nullptr);
    localtime_r(&result, &tm_tmp);
    snprintf(buffer, sizeof(buffer),
             "Heartbeat at %02d%02d%02d %2d:%02d:%02d\n", tm_tmp.tm_year % 100,
             tm_tmp.tm_mon + 1, tm_tmp.tm_mday, tm_tmp.tm_hour, tm_tmp.tm_min,
             tm_tmp.tm_sec);
    my_write(con->heartbeat_file, (uchar *)buffer, strlen(buffer), MYF(0));
  }

  return nullptr;
}

/*
  Initialize the daemon example at server start or plugin installation.

  SYNOPSIS
    daemon_example_plugin_init()

  DESCRIPTION
    Starts up heartbeatbeat thread

  RETURN VALUE
    0                    success
    1                    failure (cannot happen)
*/

static int daemon_example_plugin_init(void *p) {
  DBUG_TRACE;

#ifdef HAVE_PSI_INTERFACE
  init_deamon_example_psi_keys();
#endif

  struct mysql_heartbeat_context *con;
  my_thread_attr_t attr; /* Thread attributes */
  char heartbeat_filename[FN_REFLEN];
  char buffer[HEART_STRING_BUFFER];
  time_t result = time(nullptr);
  struct tm tm_tmp;

  struct st_plugin_int *plugin = (struct st_plugin_int *)p;

  con = (struct mysql_heartbeat_context *)my_malloc(
      key_memory_mysql_heartbeat_context,
      sizeof(struct mysql_heartbeat_context), MYF(0));

  fn_format(heartbeat_filename, "mysql-heartbeat", "", ".log",
            MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  unlink(heartbeat_filename);
  con->heartbeat_file = my_open(heartbeat_filename, O_CREAT | O_RDWR, MYF(0));

  /*
    No threads exist at this point in time, so this is thread safe.
  */
  localtime_r(&result, &tm_tmp);
  snprintf(buffer, sizeof(buffer),
           "Starting up at %02d%02d%02d %2d:%02d:%02d\n", tm_tmp.tm_year % 100,
           tm_tmp.tm_mon + 1, tm_tmp.tm_mday, tm_tmp.tm_hour, tm_tmp.tm_min,
           tm_tmp.tm_sec);
  my_write(con->heartbeat_file, (uchar *)buffer, strlen(buffer), MYF(0));

  my_thread_attr_init(&attr);
  my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_JOINABLE);

  /* now create the thread */
  if (my_thread_create(&con->heartbeat_thread, &attr, mysql_heartbeat,
                       (void *)con) != 0) {
    fprintf(stderr, "Could not create heartbeat thread!\n");
    exit(0);
  }
  plugin->data = (void *)con;

  if (is_spectrum_storage_node()) {
    grpc::ServerBuilder serverBuilder;

    grpc::Service *service = new StorageNodeImpl();
    serverBuilder.RegisterService(service);

    std::string server_address("0.0.0.0:64000");
    serverBuilder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  
    con->server = serverBuilder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;
  }

  return 0;
}

/*
  Terminate the daemon example at server shutdown or plugin deinstallation.

  SYNOPSIS
    daemon_example_plugin_deinit()
    Does nothing.

  RETURN VALUE
    0                    success
    1                    failure (cannot happen)

*/

static int daemon_example_plugin_deinit(void *p) {
  DBUG_TRACE;
  char buffer[HEART_STRING_BUFFER];
  struct st_plugin_int *plugin = (struct st_plugin_int *)p;
  struct mysql_heartbeat_context *con =
      (struct mysql_heartbeat_context *)plugin->data;
  time_t result = time(nullptr);
  struct tm tm_tmp;
  void *dummy_retval;

  my_thread_cancel(&con->heartbeat_thread);

  localtime_r(&result, &tm_tmp);
  snprintf(buffer, sizeof(buffer),
           "Shutting down at %02d%02d%02d %2d:%02d:%02d\n",
           tm_tmp.tm_year % 100, tm_tmp.tm_mon + 1, tm_tmp.tm_mday,
           tm_tmp.tm_hour, tm_tmp.tm_min, tm_tmp.tm_sec);
  my_write(con->heartbeat_file, (uchar *)buffer, strlen(buffer), MYF(0));

  /*
    Need to wait for the hearbeat thread to terminate before closing
    the file it writes to and freeing the memory it uses
  */
  my_thread_join(&con->heartbeat_thread, &dummy_retval);

  my_close(con->heartbeat_file, MYF(0));

  my_free(con);

  return 0;
}

struct st_mysql_daemon daemon_example_plugin = {MYSQL_DAEMON_INTERFACE_VERSION};

/*
  Plugin library descriptor
*/

mysql_declare_plugin(storage_agent){
    MYSQL_DAEMON_PLUGIN,
    &daemon_example_plugin,
    "daemon_example",
    PLUGIN_AUTHOR_ORACLE,
    "Daemon example, creates a heartbeat beat file in mysql-heartbeat.log",
    PLUGIN_LICENSE_GPL,
    daemon_example_plugin_init,   /* Plugin Init */
    nullptr,                      /* Plugin Check uninstall */
    daemon_example_plugin_deinit, /* Plugin Deinit */
    0x0100 /* 1.0 */,
    nullptr, /* status variables                */
    nullptr, /* system variables                */
    nullptr, /* config options                  */
    0,       /* flags                           */
} mysql_declare_plugin_end;
