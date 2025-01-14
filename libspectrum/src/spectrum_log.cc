/*****************************************************************************

Copyright (c) 2000, 2024, Oracle and/or its affiliates.
Copyright (c) 2008, 2009 Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2012, Facebook Inc.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is designed to work with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have either included with
the program or referenced in the documentation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <my_compare.h>
#include <stdlib.h>
#include <strfunc.h>
#include <time.h>
#include <algorithm>
#include <cstdint>
#include <memory>

#include <sql/sql_base.h>
#include <sql/mdl.h>
#include <sql/field.h>
#include <sql/table.h>
#include <sql/log.h>
#include <sql/sql_class.h>
#include <sql_table.h>
#include <sql/handler.h>
#include <sql/mysqld.h>
#include <sql/protocol_classic.h>

#include <current_thd.h>
#include <debug_sync.h>
#include <derror.h>
#include <my_bitmap.h>
#include <my_check_opt.h>
#include <mysql/service_thd_alloc.h>
#include <mysql/service_thd_wait.h>
#include <mysql/plugin.h>
#include <mysql_com.h>
#include <sql_string.h>
#include <sql_tablespace.h>
#include <sql_thd_internal_api.h>

#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include "google/protobuf/text_format.h"
#include "spectrum.h"
#include "spectrum_config.h"
#include "spectrum.grpc.pb.h"

typedef uint64 event_id_t;
std::atomic<event_id_t> atomic_event_id;
inline event_id_t next_event_id() {
  return ++atomic_event_id;
}

std::unique_ptr<spectrum::StorageReplicaNode::Stub> storage_replica_client;
spectrum::StorageReplicaNode::Stub* get_storage_replica_client() {
  if (!storage_replica_client) {
    node_config_t* node_config = find_storage_replica_node_config();
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(node_config->address, grpc::InsecureChannelCredentials());
    storage_replica_client = spectrum::StorageReplicaNode::NewStub(channel);
  }
  return storage_replica_client.get();
}

std::unique_ptr<grpc::ClientReaderWriter<spectrum::ReplicateRequest, spectrum::ReplicateResponse>> storage_replica_stream;
grpc::ClientReaderWriter<spectrum::ReplicateRequest, spectrum::ReplicateResponse> *get_storage_replica_stream() {
  if (!storage_replica_stream) {
    grpc::ClientContext *stream_context = new grpc::ClientContext();
    storage_replica_stream = get_storage_replica_client()->Replicate(stream_context);
  }
  return storage_replica_stream.get();
}

TABLE *find_or_open_event_table(THD *thd) {
  const char *db_name = "spectrum";
  const char *table_name = "events";

  TABLE *table = spectrum_find_or_open_table(thd, db_name, table_name, thr_lock_type::TL_WRITE, thr_locked_row_action::THR_DEFAULT);
  assert(table);

  table->use_all_columns();
  table->reginfo.lock_type = thr_lock_type::TL_WRITE;

  MDL_key mdl_key;
  mdl_key.mdl_key_init(MDL_key::enum_mdl_namespace::TABLE, db_name, table_name);

  MDL_request mdl_request;
  MDL_REQUEST_INIT_BY_KEY(&mdl_request, &mdl_key, enum_mdl_type::MDL_SHARED_WRITE, enum_mdl_duration::MDL_TRANSACTION);
  thd->mdl_context.acquire_lock(&mdl_request, 10000);
  return table;
}

int spectrum_log_build_event(my_xid xid, uint64 event_id, spectrum::event_type_enum event_type, ::google::protobuf::Message &event_body, spectrum::Event *event) {
  event->set_xid(xid);
  event->set_id(event_id);
  event->set_type(event_type);

  google::protobuf::TextFormat::Printer printer;
  printer.SetSingleLineMode(true);
  printer.PrintToString(event_body, event->mutable_body());
  return 0;
}

int spectrum_log_write_event(THD *thd, spectrum::Event *event) {
  TABLE *event_table = find_or_open_event_table(thd);
  
  MYSQL_LOCK *sql_lock = mysql_lock_tables(thd, &event_table, 1, 0);
  thd->lock = thd->lock ? mysql_lock_merge(thd->lock, sql_lock) : sql_lock;

  spectrum_debug = true;

  memset(event_table->record[0], 0, event_table->s->null_bytes);
  event_table->field[0]->store(event->xid(), true);
  event_table->field[1]->store(event->id(), true);
  event_table->field[2]->store(event->body().data(), event->body().length(), event_table->field[2]->charset());
  event_table->file->ha_write_row(event_table->record[0]);

  mysql_unlock_some_tables(thd, &event_table, 1);
  return 0;
}

int spectrum_log_replicate_event(THD* thd, spectrum::Event *event, bool wait_response) {
  spectrum::ReplicateRequest request;
  request.mutable_event()->CopyFrom(*event);

  if (!get_storage_replica_stream()->Write(request)) {
    return HA_ERR_GENERIC;
  }

  spectrum::ReplicateResponse response;
  if (wait_response) {
    do {
      if (!get_storage_replica_stream()->Read(&response)) {
        sql_print_error("spectrum_log_prepare: stream read error");
        return HA_ERR_GENERIC;
      }
    } while(event->id() != response.event_id());
  }
  return 0;
}

int spectrum_log_create_table(THD *thd, const char* db_name, const char* table_name, uint64 handler_id) {
  my_xid xid = thd->get_transaction()->xid_state()->get_xid()->get_my_xid();
  event_id_t event_id = next_event_id();

  sql_print_information("spectrum_log_create_table[%s:%s:%d]: satrt", db_name, table_name, handler_id);

  spectrum::CreateTableRequest event_body;
  spectrum_thread_fill(thd, event_body.mutable_thread());
  event_body.set_database(db_name);
  event_body.set_table(table_name);
  event_body.set_handler(handler_id);

  spectrum::Event event;
  spectrum_log_build_event(xid, event_id, spectrum::event_type_enum::CREATE_TABLE, event_body, &event);
  spectrum_log_write_event(thd, &event);
  if (spectrum_log_replicate_event(thd, &event, false)) {
    sql_print_error("spectrum_log_create_table[%s:%s:%d]: stream write error",
        event_body.database().c_str(), event_body.table().c_str(), handler_id);
  }
  return 0;
}

int spectrum_log_delete_table(THD *thd, const char* db_name, const char* table_name, const char* table_path) {
  my_xid xid = thd->get_transaction()->xid_state()->get_xid()->get_my_xid();
  event_id_t event_id = next_event_id();

  sql_print_information("spectrum_log_delete_table[%s:%s]: table_path=%s", db_name, table_name, table_path);

  spectrum::DeleteTableRequest event_body;
  spectrum_thread_fill(thd, event_body.mutable_thread());
  event_body.set_database(db_name);
  event_body.set_table(table_name);
  event_body.set_table_path(table_path);

  spectrum::Event event;
  spectrum_log_build_event(xid, event_id, spectrum::event_type_enum::DELETE_TABLE, event_body, &event);
  spectrum_log_write_event(thd, &event);
  if (spectrum_log_replicate_event(thd, &event, false)) {
    sql_print_error("spectrum_log_delete_table[%s:%s]: stream write error", db_name, table_name);
  }
  return 0;
}

int spectrum_log_post_ddl(THD *thd) {
  my_xid xid = thd->get_transaction()->xid_state()->get_xid()->get_my_xid();
  event_id_t event_id = next_event_id();

  sql_print_information("spectrum_log_post_ddl: satrt");

  spectrum::PostDDLRequest event_body;
  spectrum_thread_fill(thd, event_body.mutable_thread());

  spectrum::Event event;
  spectrum_log_build_event(xid, event_id, spectrum::event_type_enum::POST_DDL, event_body, &event);
  spectrum_log_write_event(thd, &event);
  if (spectrum_log_replicate_event(thd, &event, false)) {
    sql_print_error("spectrum_log_post_ddl: stream write error");
    return HA_ERR_GENERIC;
  }
  return 0;
}

int spectrum_log_update_metadata(THD *thd, const char* table, dd::Object_id object_id, const char* object_name) {
  my_xid xid = thd->get_transaction()->xid_state()->get_xid()->get_my_xid();
  event_id_t event_id = next_event_id();

  sql_print_information("spectrum_log_update_metadata: table=%s, object_name=%s, object_id=%d",
      table, object_name, object_id);

  spectrum::UpdateMetadataRequest event_body;
  spectrum_thread_fill(thd, event_body.mutable_thread());
  event_body.set_table(table);
  event_body.set_object_id(object_id);
  event_body.set_object_name(object_name);

  spectrum::Event event;
  spectrum_log_build_event(xid, event_id, spectrum::event_type_enum::UPDATE_METADATA, event_body, &event);
  spectrum_log_write_event(thd, &event);
  if (spectrum_log_replicate_event(thd, &event, false)) {
    sql_print_error("spectrum_log_update_metadata: stream write error");
    return HA_ERR_GENERIC;
  }
  return 0;
}

int spectrum_log_add_row(THD *thd, TABLE *table, uchar *new_row, uchar *old_row) {
  my_xid xid = thd->get_transaction()->xid_state()->get_xid()->get_my_xid();
  event_id_t event_id = next_event_id();

  spectrum_print_row("spectrum_log_add_row_new", table, new_row);
  spectrum_print_row("spectrum_log_add_old_new", table, old_row);

  spectrum::ReplicateRowRequest event_body;
  spectrum_thread_fill(thd, event_body.mutable_thread());
  event_body.set_database(table->s->db.str);
  event_body.set_table(table->s->table_name.str);
  event_body.set_handler((uint64)table->file);
  event_body.set_lock_type(table->reginfo.lock_type);
  event_body.set_lock_action(table->pos_in_table_list->lock_descriptor().type);
  if (new_row) spectrum_row_fill_fields(table, new_row, event_body.mutable_new_row());
  if (old_row) spectrum_row_fill_fields(table, old_row, event_body.mutable_old_row());

  spectrum::Event event;
  spectrum_log_build_event(xid, event_id, spectrum::event_type_enum::ADD_ROW, event_body, &event);
  spectrum_log_write_event(thd, &event);
  if (spectrum_log_replicate_event(thd, &event, false)) {
    sql_print_error("spectrum_log_add_row[%s:%s:%d]: stream write error", table->s->db.str, table->s->table_name.str, table->file);
    return HA_ERR_GENERIC;
  }
  return 0;
}

int spectrum_log_prepare(THD *thd, bool all) {
  my_xid xid = thd->get_transaction()->xid_state()->get_xid()->get_my_xid();
  event_id_t event_id = next_event_id();

  sql_print_information("spectrum_log_prepare: all=%d", all);

  spectrum::PrepareRequest event_body;
  spectrum_thread_fill(thd, event_body.mutable_thread());
  event_body.set_all(all);

  spectrum::Event event;
  spectrum_log_build_event(xid, event_id, spectrum::event_type_enum::PREPARE, event_body, &event);
  //spectrum_log_write_event(thd, &event);
  bool wait_response = all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
  if (spectrum_log_replicate_event(thd, &event, wait_response)) {
    sql_print_error("spectrum_log_prepare: stream write error");
    return HA_ERR_GENERIC;
  }
  return 0;
}

int spectrum_log_commit(THD *thd, bool all) {
  my_xid xid = thd->get_transaction()->xid_state()->get_xid()->get_my_xid();
  event_id_t event_id = next_event_id();

  sql_print_information("spectrum_log_commit: all=%d", all);

  spectrum::CommitRequest event_body;
  spectrum_thread_fill(thd, event_body.mutable_thread());
  event_body.set_all(all);

  spectrum::Event event;
  spectrum_log_build_event(xid, event_id, spectrum::event_type_enum::COMMIT, event_body, &event);
  //spectrum_log_write_event(thd, &event);
  bool wait_response = all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
  if (spectrum_log_replicate_event(thd, &event, wait_response)) {
    sql_print_error("spectrum_log_commit: stream write error");
    return HA_ERR_GENERIC;
  }
  return 0;
}