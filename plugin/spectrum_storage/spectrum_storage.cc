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
#include "sql/dd/dd_table.h"
#include "sql/dd_table_share.h"
#include "sql/dd/types/table.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dd_schema.h"
#include "sql/dd/impl/cache/shared_dictionary_cache.h" 
#include "sql/mysqld_thd_manager.h"

#include <spectrum.h>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include "spectrum.grpc.pb.h"

char thread_stack = 'a';

class Find_thd_with_spectrum_thread_id : public Find_THD_Impl {
 public:
  explicit Find_thd_with_spectrum_thread_id(uint64 spectrum_thread_id)
      : m_spectrum_thread_id(spectrum_thread_id) {}
  bool operator()(THD *thd) override {
    if (thd->spectrum_thread_id == m_spectrum_thread_id) {
      return true;
    }
    return false;
  }
 private:
  const uint64 m_spectrum_thread_id;
};

THD *create_thd(const spectrum::Thread &spectrum_thread)
{
  my_thread_init();

  Find_thd_with_spectrum_thread_id find_thd_with_spectrum_thread_id(spectrum_thread.id());
  THD_ptr thd_ptr = Global_THD_manager::get_instance()->find_thd(&find_thd_with_spectrum_thread_id);
  THD *thd = thd_ptr.get();
  if (!thd) {
    thd = new (std::nothrow) THD;
    thd->spectrum_thread_id = spectrum_thread.id();
    thd->set_new_thread_id();
    thd->thread_stack = reinterpret_cast<char *>(&thread_stack);
    thd->get_protocol_classic()->init_net((Vio *)0);
    Global_THD_manager::get_instance()->add_thd(thd);

    // This is needed because register_uncommitted_object() and register_dropped_object() require a non-default
    // auto releaser, even though it's not actually required because uncommitted/dropped objects will be
    // released in remove_uncommitted_objects() when transaction commits.
    new dd::cache::Dictionary_client::Auto_releaser(thd->dd_client());

    sql_print_information("Created new thread: spectrum_thread_id=%d local_thread_id=%d",
        spectrum_thread.id(), thd->thread_id()); 
  }
  thd->store_globals();
  thd->lex->sql_command = (enum_sql_command)spectrum_thread.sql_command();
  thd->tx_isolation = (enum_tx_isolation)spectrum_thread.tx_isolation();
  thd->query_id = (query_id_t)spectrum_thread.query_id();
  thd->variables.option_bits = spectrum_thread.system_variables().option_bits();

  return (thd);
}

TABLE *open_table(
    THD *thd,
    const char *db_name,
    const char *table_name,
    uint64 handler_id,
    thr_lock_type lock_type,
    thr_locked_row_action lock_action)
{
  Open_table_context otc(thd, 0);

  Table_ref *tables = new Table_ref(db_name, strlen(db_name), table_name, strlen(table_name),
                  table_name, lock_type);
  tables->set_lock({lock_type, lock_action});
  if (!open_table(thd, tables, &otc)) {
    TABLE *table = tables->table;
    table->use_all_columns();
    table->file->spectrum_handler_id = handler_id;
    return table;
  }
  return nullptr;
}

TABLE *find_or_open_table(
    THD *thd,
    const char *db_name,
    const char *table_name,
    uint64 handler_id,
    thr_lock_type lock_type,
    thr_locked_row_action lock_action)
{
  for (TABLE *t = thd->open_tables; t; t = t->next) {
    if (t->file->spectrum_handler_id == handler_id) {
      assert(!strcmp(t->s->db.str, db_name) && !strcmp(t->s->table_name.str, table_name));
      return t;
    }
  }
  return open_table(thd, db_name, table_name, handler_id, lock_type, lock_action);
}

void find_and_close_table(
    THD *thd,
    const char *db_name,
    const char *table_name,
    uint64 handler_id)
{
  TABLE **table;
  for (table = &thd->open_tables; *table; table = &(*table)->next) {
    if ((*table)->file->spectrum_handler_id == handler_id) {
      assert(!strcmp((*table)->s->db.str, db_name) && !strcmp((*table)->s->table_name.str, table_name));
      break;
    }
  }
  assert(*table);

  close_thread_table(thd, table);
}

bool check_and_coalesce_trx_read_write(THD *thd, bool all) {
  Transaction_ctx::enum_trx_scope trx_scope =
          all ? Transaction_ctx::SESSION : Transaction_ctx::STMT;
  auto ha_list = thd->get_transaction()->ha_trx_info(trx_scope);

  for (auto const &ha_info : ha_list) {
    if (!all) {
      Ha_trx_info *ha_info_all =
          &thd->get_ha_data(ha_info.ht()->slot)->ha_info[1];
      assert(&ha_info != ha_info_all);
      if (ha_info_all->is_started()) {
        ha_info_all->coalesce_trx_with(ha_info);
      }
    }
    if (ha_info.is_trx_read_write()) {
      return true;
    }
  }
  return false;
}

int create_table(THD *thd, const char* db_name, const char* table_name, uint64 handler_id) {
  HA_CREATE_INFO create_info;
  dd::Table *table_def = nullptr;
  char table_filepath[FN_REFLEN + 1];

  sql_print_information("CreateTable[%s:%s:%d]", db_name, table_name, handler_id);

  // Retrive table definition
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  thd->dd_client()->acquire(db_name, table_name, (const dd::Abstract_table **)&table_def);
  if (table_def == nullptr) {
    sql_print_error("CreateTable[%s:%s:%d]: can not find table definition", db_name, table_name, handler_id);
    return HA_ERR_NO_SUCH_TABLE;
  }

  build_table_filename(table_filepath, sizeof(table_filepath) - 1, db_name, table_name, "", 0);      
  int error = ha_create_table(thd, table_filepath, db_name, table_name, &create_info, true, false, table_def);
  if (error) {
    sql_print_error("CreateTable[%s:%s:%d]: can not create table in ha, error=%d", db_name, table_name, handler_id, error);
    return error;
  }
  return 0;
}

int delete_table(THD *thd, const char* db_name, const char* table_name, const char* table_path) {
  handlerton *hton{nullptr};
  const dd::Table *table_def = nullptr;

  sql_print_information("DeleteTable[%s:%s]: table_path=%s", db_name, table_name, table_path);

  // Retrive table definition
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  thd->dd_client()->acquire(db_name, table_name, (const dd::Abstract_table **)&table_def);
  if (table_def == nullptr) {
    sql_print_error("DeleteTable[%s:%s]: can not find table definition", db_name, table_name);
    return HA_ERR_NO_SUCH_TABLE;
  }

  tdc_remove_table(thd, TDC_RT_REMOVE_ALL, db_name, table_name, false);

  dd::table_storage_engine(thd, table_def, &hton);
  int error = ha_delete_table(thd, hton, table_path, db_name, table_name, table_def->clone(), false);
  if (error) {
    sql_print_error("DeleteTable[%s:%s]: can not delete table in ha, error=%d", db_name, table_name, error);
    return error;
  }
  return 0;
}

int update_metadata(THD *thd, const char* table_name, dd::Object_id object_id, const char* object_name) {
  sql_print_information("UpdateMetadata: table=%s, object_id=%d, object_name=%s",
        table_name, object_id, object_name);
  if (!strcmp(table_name, "schemata")) {
    const dd::Schema *object;
    thd->dd_client()->reload_uncommitted(object_id, &object);
  } else if (!strcmp(table_name, "tables")) {
    const dd::Abstract_table *object;
    thd->dd_client()->reload_uncommitted(object_id, &object);
  }
  return 0;
}

int post_ddl(THD *thd) {
  sql_print_information("PostDDL");

  handlerton *hton = ha_default_handlerton(thd);
  hton->post_ddl(thd);
  return 0;
}

class StorageNodeImpl final : public spectrum::StorageNode::Service {
  private:
    MDL_ticket *find_ticket_by_number(THD* thd, enum_mdl_duration duration, int32 ticket_number) {
      MDL_ticket *ticket = nullptr;
      MDL_context::Ticket_iterator ticket_it = thd->mdl_context.get_tickets_for_duration(duration);
      for (ticket = ticket_it++; ticket != nullptr; ticket = ticket_it++) {
        if (ticket->ticket_number == ticket_number) {
          break;
        }
      }
      return ticket;
    }

  public:
    ::grpc::Status CreateTable(::grpc::ServerContext* context, const ::spectrum::CreateTableRequest* request, ::spectrum::CreateTableResponse* response) {
      THD *thd;
      const char* db_name = request->database().c_str();
      const char* table_name = request->table().c_str();
      uint64 handler_id = request->handler();

      thd = create_thd(request->thread());
      create_table(thd, db_name, table_name, handler_id);

      spectrum_log_create_table(thd, db_name, table_name, handler_id);
      return grpc::Status::OK; 
    }

    ::grpc::Status DeleteTable(::grpc::ServerContext* context, const ::spectrum::DeleteTableRequest* request, ::spectrum::DeleteTableResponse* response) {
      THD *thd;
      const char* db_name = request->database().c_str();
      const char* table_name = request->table().c_str();
      const char* table_path = request->table_path().c_str();

      thd = create_thd(request->thread());
      delete_table(thd, db_name, table_name, table_path);

      spectrum_log_delete_table(thd, db_name, table_name, table_path);
      return grpc::Status::OK; 
    }

    ::grpc::Status PostDDL(::grpc::ServerContext* context, const ::spectrum::PostDDLRequest* request, ::spectrum::PostDDLResponse* response) {
      THD *thd = create_thd(request->thread());
      post_ddl(thd);

      spectrum_log_post_ddl(thd);
      return grpc::Status::OK; 
    }

    ::grpc::Status LockTable(::grpc::ServerContext* context, const ::spectrum::LockTableRequest* request, ::spectrum::LockTableResponse* response) {
      THD *thd;
      TABLE *table;
      const char* db_name = request->database().c_str();
      const char* table_name = request->table().c_str();
      uint64 handler_id = request->handler();
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();

      sql_print_information("LockTable[%s:%s:%d]: lock_type=%d", db_name, table_name, handler_id, lock_type);

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, db_name, table_name, handler_id, lock_type, lock_action);

      table->reginfo.lock_type = lock_type;
      MYSQL_LOCK *lock = mysql_lock_tables(thd, &table, 1, 0);
      thd->lock = thd->lock ? mysql_lock_merge(thd->lock, lock) : lock;

      return grpc::Status::OK; 
    }

    ::grpc::Status UnlockTable(::grpc::ServerContext* context, const ::spectrum::UnlockTableRequest* request, ::spectrum::UnlockTableResponse* response) {
      THD *thd;
      TABLE *table;
      const char* db_name = request->database().c_str();
      const char* table_name = request->table().c_str();
      uint64 handler_id = request->handler();
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();

      sql_print_information("UnLockTable[%s:%s:%d]", db_name, table_name, handler_id);

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, db_name, table_name, handler_id, lock_type, lock_action);

      mysql_unlock_some_tables(thd, &table, 1);

      return grpc::Status::OK; 
    }

    ::grpc::Status CloseTable(::grpc::ServerContext* context, const ::spectrum::CloseTableRequest* request, ::spectrum::CloseTableResponse* response) {
      THD *thd;
      TABLE *table;
      const char* db_name = request->database().c_str();
      const char* table_name = request->table().c_str();
      uint64 handler_id = request->handler();

      sql_print_information("CloseTable[%s:%s:%d]", db_name, table_name, handler_id);

      thd = create_thd(request->thread());
      find_and_close_table(thd, db_name, table_name, handler_id);

      return grpc::Status::OK; 
    }

    ::grpc::Status InitIndex(::grpc::ServerContext* context, const ::spectrum::InitIndexRequest* request, ::spectrum::InitIndexResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();

      sql_print_information("InitIndex[%s:%s:%d]: index=%d", request->database().c_str(), request->table().c_str(), request->handler(), request->index());

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);

      table->file->ha_index_init(request->index(), true);

      return grpc::Status::OK; 
    }

    ::grpc::Status InitRnd(::grpc::ServerContext* context, const ::spectrum::InitRndRequest* request, ::spectrum::InitRndResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();

      sql_print_information("InitRnd[%s:%s:%d]: scan=%d", request->database().c_str(), request->table().c_str(), request->handler(), request->scan());

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);

      table->file->ha_rnd_init(request->scan());

      return grpc::Status::OK; 
    }

    ::grpc::Status EndIndex(::grpc::ServerContext* context, const ::spectrum::EndIndexRequest* request, ::spectrum::EndIndexResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();

      sql_print_information("EndIndex[%s:%s:%d]", request->database().c_str(), request->table().c_str(), request->handler());

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);

      if (table->file->inited == handler::RND) {
        table->file->ha_rnd_end();
      } else if (table->file->inited == handler::INDEX) {
        table->file->ha_index_end();
      }

      return grpc::Status::OK; 
    }

    ::grpc::Status ReadRow(::grpc::ServerContext* context, const ::spectrum::ReadRowRequest* request, ::spectrum::ReadRowResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();
      const uchar *key = nullptr;
      uint key_len = request->key_len();
      enum ha_rkey_function find_flags = (enum ha_rkey_function)request->find_flag();

      if (key_len) {
        key = (const uchar *)request->key().data();
      }

      sql_print_information("ReadRow[%s:%s:%d]", request->database().c_str(), request->table().c_str(), request->handler());

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);
      empty_record(table);
      
      int error = table->file->ha_index_read(table->record[0], key, key_len, find_flags);
      if (error != HA_ERR_KEY_NOT_FOUND) {
        spectrum_row_fill_fields(table, response->mutable_row());
      }
      spectrum_print_row("ReadRow", table);
      return grpc::Status::OK; 
    }

    ::grpc::Status ReadNextRow(::grpc::ServerContext* context, const ::spectrum::ReadNextRowRequest* request, ::spectrum::ReadNextRowResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();
      int error;

      sql_print_information("ReadNextRow[%s:%s:%d]", request->database().c_str(), request->table().c_str(), request->handler());

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);
      empty_record(table);

      if (table->file->inited == handler::RND) {
        error = table->file->ha_rnd_next(table->record[0]);
      } else if (table->file->inited == handler::INDEX) {
        if (request->same()) {
          error = table->file->ha_index_next_same(table->record[0], nullptr, 0);
        } else {
          error = table->file->ha_index_next(table->record[0]);
        }
      }
      if (error != HA_ERR_END_OF_FILE) {
        spectrum_row_fill_fields(table, response->mutable_row());
      }
      spectrum_print_row("ReadNextRow", table);
      return grpc::Status::OK; 
    }

    ::grpc::Status ReadPrevRow(::grpc::ServerContext* context, const ::spectrum::ReadPrevRowRequest* request, ::spectrum::ReadPrevRowResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();
      int error;

      sql_print_information("ReadPrevRow[%s:%s:%d]", request->database().c_str(), request->table().c_str(), request->handler());

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);
      empty_record(table);

      error = table->file->ha_index_prev(table->record[0]);
      if (error != HA_ERR_END_OF_FILE) {
        spectrum_row_fill_fields(table, response->mutable_row());
      }
      spectrum_print_row("ReadPrevRow", table);
      return grpc::Status::OK; 
    }

    ::grpc::Status WriteRow(::grpc::ServerContext* context, const ::spectrum::WriteRowRequest* request, ::spectrum::WriteRowResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);
      empty_record(table);
      
      table->autoinc_field_has_explicit_non_null_value = request->autoinc_field_has_explicit_non_null_value();

      ::spectrum::Row spectrum_row = request->row();
      spectrum_row_extract_fields(table, &spectrum_row);
      spectrum_print_row("WriteRow", table);

      // For autoincr field to work
      table->next_number_field = table->found_next_number_field;

      table->file->ha_write_row(table->record[0]);
      response->set_insert_id(table->file->insert_id_for_cur_row);
      table->file->ha_release_auto_increment();

      spectrum_print_row("WriteRowNew", table);
      spectrum_row_fill_fields(table, response->mutable_row());

      spectrum_log_add_row(thd, table, table->record[0], nullptr);
      return grpc::Status::OK; 
    }

    ::grpc::Status UpdateRow(::grpc::ServerContext* context, const ::spectrum::UpdateRowRequest* request, ::spectrum::UpdateRowResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);
      empty_record(table);
      table->autoinc_field_has_explicit_non_null_value = request->autoinc_field_has_explicit_non_null_value();

      ::spectrum::Row spectrum_old_row = request->old_row();
      spectrum_row_extract_fields(table, table->record[1], &spectrum_old_row);
      ::spectrum::Row spectrum_new_row = request->new_row();
      spectrum_row_extract_fields(table, table->record[0], &spectrum_new_row);
      spectrum_print_row("UpdateRow", table);

      // For autoincr field to work
      table->next_number_field = table->found_next_number_field;

      table->file->ha_update_row(table->record[1], table->record[0]);
      table->file->ha_release_auto_increment();

      spectrum_log_add_row(thd, table, table->record[0], table->record[1]);
      return grpc::Status::OK; 
    }

    ::grpc::Status DeleteRow(::grpc::ServerContext* context, const ::spectrum::DeleteRowRequest* request, ::spectrum::DeleteRowResponse* response) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = (thr_lock_type)request->lock_type();
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);
      empty_record(table);

      ::spectrum::Row spectrum_row = request->row();
      spectrum_row_extract_fields(table, &spectrum_row);
      spectrum_print_row("DeleteRow", table);

      table->file->ha_delete_row(table->record[0]);

      spectrum_log_add_row(thd, table, nullptr, table->record[0]);
      return grpc::Status::OK;
    }

    ::grpc::Status Prepare(::grpc::ServerContext* context, const ::spectrum::PrepareRequest* request, ::spectrum::PrepareResponse* response) {
      THD *thd = create_thd(request->thread());
      bool all = request->all();

      sql_print_information("Prepare[%d]: all=%d", thd->spectrum_thread_id, all);

      if (check_and_coalesce_trx_read_write(thd, all)) {
        spectrum_log_prepare(thd, all);
      } else {
        sql_print_information("Prepare[%d]: skip spectrum log prepare for readonly transaction", thd->spectrum_thread_id);
      }

      ha_prepare_low(thd, all);
      return grpc::Status::OK; 
    }

    ::grpc::Status Commit(::grpc::ServerContext* context, const ::spectrum::CommitRequest* request, ::spectrum::CommitResponse* response) {
      THD *thd = create_thd(request->thread());
      bool all = request->all();

      sql_print_information("Commit[%d]: all=%d", thd->spectrum_thread_id, all);

      if (check_and_coalesce_trx_read_write(thd, all)) {
        spectrum_log_commit(thd, all);
      } else {
        sql_print_information("Commit[%d]: skip spectrum log commit for readonly transaction", thd->spectrum_thread_id);
      }
  
      ha_commit_low(thd, all, false);
      return grpc::Status::OK; 
    }

    ::grpc::Status BeginAttachableTransaction(::grpc::ServerContext* context, const ::spectrum::BeginAttachableTransactionRequest* request, ::spectrum::BeginAttachableTransactionResponse* response) {
      THD *thd;

      sql_print_information("BeginAttachableTransaction: readonly=%d", request->readonly());

      thd = create_thd(request->thread());

      if (request->readonly()) {
        thd->begin_attachable_ro_transaction();
      } else {
        thd->begin_attachable_rw_transaction();
      }
      return grpc::Status::OK; 
    }

    ::grpc::Status EndAttachableTransaction(::grpc::ServerContext* context, const ::spectrum::EndAttachableTransactionRequest* request, ::spectrum::EndAttachableTransactionResponse* response) {
      THD *thd;

      sql_print_information("EndAttachableTransaction");

      thd = create_thd(request->thread());

      thd->end_attachable_transaction();

      return grpc::Status::OK; 
    }

    ::grpc::Status UpdateMetadata(::grpc::ServerContext* context, const ::spectrum::UpdateMetadataRequest* request, ::spectrum::UpdateMetadataResponse* response) {
      THD *thd;
      const std::string& table = request->table();
      const dd::Object_id object_id = request->object_id();
      const std::string& object_name = request->object_name();

      thd = create_thd(request->thread());
      update_metadata(thd, table.c_str(), object_id, object_name.c_str());

      spectrum_log_update_metadata(thd, table.c_str(), object_id, object_name.c_str());
      return grpc::Status::OK; 
    }

    ::grpc::Status AcquireMetadataLock(::grpc::ServerContext* context, const ::spectrum::AcquireMetadataLockRequest* request, ::spectrum::AcquireMetadataLockResponse* response) {
      THD *thd;
      MDL_key::enum_mdl_namespace namespace_ = static_cast<MDL_key::enum_mdl_namespace>(request->namespace_());
      const std::string& schema = request->schema();
      const std::string& table = request->table();
      const std::string& column = request->column();
      enum_mdl_type type = static_cast<enum_mdl_type>(request->type());
      enum_mdl_duration duration = static_cast<enum_mdl_duration>(request->duration());
      int32_t ticket_number = request->ticket_number();

      sql_print_information("AcquireMetadataLock: namespace=%d, db=%s, table=%s, column=%s, type=%d, duration=%d, ticket_number=%d",
          namespace_, schema.c_str(), table.c_str(), column.c_str(), type, duration, ticket_number); 

      thd = create_thd(request->thread());
  
      MDL_key mdl_key;
      if (column.length()) {
        mdl_key.mdl_key_init(namespace_, schema.c_str(), table.c_str(), column.c_str());
      } else {
        mdl_key.mdl_key_init(namespace_, schema.c_str(), table.c_str());
      }

      MDL_request mdl_request;
      MDL_REQUEST_INIT_BY_KEY(&mdl_request, &mdl_key, type, duration);
      thd->mdl_context.acquire_lock(&mdl_request, 10000);
      mdl_request.ticket->ticket_number = ticket_number;
      return grpc::Status::OK; 
    }

    ::grpc::Status UpgradeMetadataLock(::grpc::ServerContext* context, const ::spectrum::UpgradeMetadataLockRequest* request, ::spectrum::UpgradeMetadataLockResponse* response) {
      THD *thd;
      enum_mdl_duration duration = static_cast<enum_mdl_duration>(request->duration());
      int32_t ticket_number = request->ticket_number();
      enum_mdl_type new_type = static_cast<enum_mdl_type>(request->new_type());

      sql_print_information("UpgradeMetadataLock: duration=%d, ticket_number=%d, new_type=%d", duration, ticket_number, new_type);

      thd = create_thd(request->thread());

      MDL_ticket *ticket = find_ticket_by_number(thd, duration, ticket_number);
      if (ticket) {
        thd->mdl_context.upgrade_shared_lock(ticket, new_type, 10000);
      }

      return grpc::Status::OK; 
    }

    ::grpc::Status ReleaseMetadataLock(::grpc::ServerContext* context, const ::spectrum::ReleaseMetadataLockRequest* request, ::spectrum::ReleaseMetadataLockResponse* response) {
      THD *thd;
      enum_mdl_duration duration = static_cast<enum_mdl_duration>(request->duration());
      int32_t ticket_number = request->ticket_number();

      sql_print_information("ReleaseMetadataLock: duration=%d, ticket_number=%d", duration, ticket_number);

      thd = create_thd(request->thread());

      MDL_ticket *ticket = find_ticket_by_number(thd, duration, ticket_number);
      if (ticket) {
        thd->mdl_context.release_lock(duration, ticket);
      }

      return grpc::Status::OK; 
    }

    ::grpc::Status ReleaseMetadataLocks(::grpc::ServerContext* context, const ::spectrum::ReleaseMetadataLocksRequest* request, ::spectrum::ReleaseMetadataLocksResponse* response) {
      THD *thd;
      bool transactional = request->transactional();

      sql_print_information("ReleaseMetadataLocks: transactional=%d", transactional);

      thd = create_thd(request->thread());

      if (transactional) {
        thd->mdl_context.release_transactional_locks();
      } else {
        thd->mdl_context.release_statement_locks();
      }

      return grpc::Status::OK; 
    }
};

class StorageReplicaNodeImpl final : public spectrum::StorageReplicaNode::Service {
  public:
    int CreateTable(const ::spectrum::CreateTableRequest* request) {
      THD *thd;
      const char* db_name = request->database().c_str();
      const char* table_name = request->table().c_str();
      uint64 handler_id = request->handler();

      thd = create_thd(request->thread());
      create_table(thd, db_name, table_name, handler_id);
      return 0;
    }

    int DeleteTable(const ::spectrum::DeleteTableRequest* request) {
      THD *thd;
      const char* db_name = request->database().c_str();
      const char* table_name = request->table().c_str();
      const char* table_path = request->table_path().c_str();

      thd = create_thd(request->thread());
      delete_table(thd, db_name, table_name, table_path);
      return 0;
    }

    int PostDDL(const ::spectrum::PostDDLRequest* request) {
      THD *thd = create_thd(request->thread());
      post_ddl(thd);
      return 0;
    }

    int UpdateMetadata(const ::spectrum::UpdateMetadataRequest* request) {
      THD *thd;
      const std::string& table = request->table();
      const dd::Object_id object_id = request->object_id();
      const std::string& object_name = request->object_name();

      thd = create_thd(request->thread());
      update_metadata(thd, table.c_str(), object_id, object_name.c_str());
      return 0;
    }

    int ReplicateRow(const ::spectrum::ReplicateRowRequest* request) {
      THD *thd;
      TABLE *table;
      thr_lock_type lock_type = TL_WRITE;
      thr_locked_row_action lock_action = (thr_locked_row_action)request->lock_action();
      uchar key[MAX_KEY_LENGTH];
      int err;

      thd = create_thd(request->thread());
      table = find_or_open_table(thd, request->database().c_str(), request->table().c_str(), request->handler(), lock_type, lock_action);
      empty_record(table);

      if (request->has_new_row()) {
        spectrum_row_extract_fields(table, table->record[0], (spectrum::Row *)&request->new_row());
      } else {
        spectrum_row_extract_fields(table, table->record[0], (spectrum::Row *)&request->old_row());
      }
      key_copy((uchar *)key, table->record[0], table->key_info + table->s->primary_key, 0);
      
      // Only lock if unlocked, ha_external_lock doesn't accept consecutive locks
      if (table->file->get_lock_type() == F_UNLCK) {
        table->reginfo.lock_type = lock_type;
        MYSQL_LOCK *lock = mysql_lock_tables(thd, &table, 1, 0);
        thd->lock = thd->lock ? mysql_lock_merge(thd->lock, lock) : lock;
      }

      if (request->has_old_row()) {
        table->file->ha_index_init(table->s->primary_key, false);
        table->file->ha_index_read_map(table->record[1], key, HA_WHOLE_KEY, HA_READ_KEY_EXACT);
        if (request->has_new_row()) {
          spectrum_print_row("ReplicateRowOld", table, table->record[1]);
          spectrum_print_row("ReplicateRowUpdate", table, table->record[0]);
          err = table->file->ha_update_row(table->record[1], table->record[0]);
        } else {
          spectrum_print_row("ReplicateRowDelete", table, table->record[1]);
          err = table->file->ha_delete_row(table->record[1]);
        }
        table->file->ha_index_end();
      } else {
        assert(request->has_new_row());
        spectrum_print_row("ReplicateRowNew", table, table->record[0]);
        err = table->file->ha_write_row(table->record[0]);
      }

      if (err) {
        sql_print_error("ReplicateRow[%s:%s:%d]: error=%d", request->database().c_str(), request->table().c_str(), request->handler(), err);
      }
      return 0;
    }

    int Prepare(const ::spectrum::PrepareRequest* request) {
      sql_print_information("Prepare: all=%d", request->all());

      THD *thd = create_thd(request->thread());
      ha_prepare_low(thd, request->all());
      return 0;
    }

    int Commit(const ::spectrum::CommitRequest* request) {
      sql_print_information("Commit: all=%d", request->all());

      THD *thd = create_thd(request->thread());

      close_thread_tables(thd);

      ha_commit_low(thd, request->all(), false);
      if (request->all()) {
        thd->mdl_context.release_transactional_locks();
      }
      return 0;
    }

    grpc::Status Replicate(grpc::ServerContext* context, grpc::ServerReaderWriter<spectrum::ReplicateResponse, spectrum::ReplicateRequest>* stream) override {
        spectrum::ReplicateRequest request;
        spectrum::ReplicateResponse response;
        while (stream->Read(&request)) {
          if (request.has_create_table_event()) {
            spectrum::CreateTableRequest event = request.create_table_event();
            CreateTable(&event);
          } else if (request.has_delete_table_event()) {
            spectrum::DeleteTableRequest event = request.delete_table_event();
            DeleteTable(&event);
          } else if (request.has_post_ddl_event()) {
            spectrum::PostDDLRequest event = request.post_ddl_event();
            PostDDL(&event);
          } else if (request.has_update_metadata_event()) {
            spectrum::UpdateMetadataRequest event = request.update_metadata_event();
            UpdateMetadata(&event);
          } else if (request.has_replicate_row_event()) {
            spectrum::ReplicateRowRequest event = request.replicate_row_event();
            ReplicateRow(&event);
          } else if (request.has_prepare_event()) {
            spectrum::PrepareRequest event = request.prepare_event();
            Prepare(&event);
            response.set_event_id(request.event_id());
            stream->Write(response);
          } else if (request.has_commit_event()) {
            spectrum::CommitRequest event = request.commit_event();
            Commit(&event);
            response.set_event_id(request.event_id());
            stream->Write(response);
          }
        }
        return grpc::Status::OK;
    }
};

struct spectrum_storage_plugin_context {
  std::unique_ptr<grpc::Server> server;
};

PSI_memory_key key_memory_spectrum_storage_plugin_context;

static char* get_spectrum_storage_node_port() {
  return getenv("SPECTRUM_STORAGE_NODE_PORT");
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
  struct spectrum_storage_plugin_context *con;
  struct st_plugin_int *plugin = (struct st_plugin_int *)p;

  con = (struct spectrum_storage_plugin_context *)my_malloc(
      key_memory_spectrum_storage_plugin_context,
      sizeof(struct spectrum_storage_plugin_context), MYF(0));
  plugin->data = (void *)con;

  if (is_spectrum_storage()) {
    grpc::ServerBuilder serverBuilder;

    grpc::Service *service = new StorageNodeImpl();
    serverBuilder.RegisterService(service);

    grpc::Service *replicaService = new StorageReplicaNodeImpl();
    serverBuilder.RegisterService(replicaService);

    std::string server_address("0.0.0.0:");
    server_address.append(get_spectrum_storage_node_port());
    serverBuilder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    con->server = serverBuilder.BuildAndStart();
    sql_print_information("Spectrum storage server started at %s", server_address.c_str());
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

  return 0;
}

struct st_mysql_daemon daemon_example_plugin = {MYSQL_DAEMON_INTERFACE_VERSION};

/*
  Plugin library descriptor
*/

mysql_declare_plugin(spectrum_storage){
    MYSQL_DAEMON_PLUGIN,
    &daemon_example_plugin,
    "spectrum_storage",
    PLUGIN_AUTHOR_ORACLE,
    "Spectrum storage",
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