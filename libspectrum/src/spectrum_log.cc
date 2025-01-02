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

#include <sql/mdl.h>
#include <sql/field.h>
#include <sql/table.h>
#include <sql/log.h>
#include <sql/sql_class.h>
#include <sql_table.h>
#include <sql/handler.h>
#include <sql/mysqld.h>
#include <sql/dd/types/schema.h>
#include <sql/dd/cache/dictionary_client.h>

#include <current_thd.h>
#include <debug_sync.h>
#include <derror.h>
#include <my_bitmap.h>
#include <my_check_opt.h>
#include <mysql/service_thd_alloc.h>
#include <mysql/service_thd_wait.h>
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
#include "spectrum.h"
#include "spectrum.grpc.pb.h"

std::unique_ptr<spectrum::StorageReplicaNode::Stub> storage_replica_client;
spectrum::StorageReplicaNode::Stub* get_storage_replica_client() {
  if (!storage_replica_client) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:64001", grpc::InsecureChannelCredentials());
    storage_replica_client = spectrum::StorageReplicaNode::NewStub(channel);
  }
  return storage_replica_client.get();
}

int spectrum_log_create_table(THD *thd, TABLE *table) {
  spectrum::CreateTableRequest request;
  spectrum::CreateTableResponse response;
  
  sql_print_information("spectrum_log_create_table[%s:%s:%d]: satrt", table->s->db.str, table->s->table_name.str, table->file);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);

  grpc::ClientContext context;
  grpc::Status status = get_storage_replica_client()->CreateTable(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_log_create_table[%s:%s:%d]: error=%s",
        request.database().c_str(), request.table().c_str(), table->file, status.error_message().c_str());
    return 1;
  }
  return 0;
}

int spectrum_log_delete_table(THD *thd, const dd::Schema *schema_def, const dd::Table *table_def, const char* table_path) {
  spectrum::DeleteTableRequest request;
  spectrum::DeleteTableResponse response;

  sql_print_information("spectrum_log_delete_table[%s:%s]: table_path=%s", schema_def->name().c_str(), table_def->name().c_str(), table_path);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(schema_def->name().c_str());
  request.set_table(table_def->name().c_str());
  request.set_table_path(table_path);

  grpc::ClientContext context;
  grpc::Status status = get_storage_replica_client()->DeleteTable(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_log_delete_table[%s:%s]: error=%s", schema_def->name().c_str(), table_def->name().c_str(), status.error_message().c_str());
  }

  return 0;
}

int spectrum_log_post_ddl(THD *thd) {
  spectrum::PostDDLRequest request;
  spectrum::PostDDLResponse response;

  sql_print_information("spectrum_log_post_ddl: satrt");

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);

  grpc::ClientContext context;
  grpc::Status status = get_storage_replica_client()->PostDDL(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_log_post_ddl: error=%s", status.error_message().c_str());
  }

  return 0;
}

int spectrum_log_update_metadata(THD *thd, const char* table, dd::Object_id object_id, const char* object_name) {
  spectrum::UpdateMetadataRequest request;
  spectrum::UpdateMetadataResponse response;
  grpc::ClientContext context;

  sql_print_information("spectrum_log_update_metadata: table=%s, object_name=%s, object_id=%d",
      table, object_name, object_id);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_table(table);
  request.set_object_id(object_id);
  request.set_object_name(object_name);

  grpc::Status status = get_storage_replica_client()->UpdateMetadata(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_log_update_metadata: error=%s", status.error_message().c_str());
  }

  return 0;
}

int spectrum_log_add_row(THD *thd, TABLE *table, uchar *new_row, uchar *old_row) {
  spectrum::ReplicateRowRequest request;
  spectrum::ReplicateRowResponse response;
  grpc::ClientContext context;

  spectrum_print_row("spectrum_log_replicate_row", table);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);
  request.set_lock_action(table->pos_in_table_list->lock_descriptor().type);

  if (new_row) spectrum_row_fill_fields(table, new_row, request.mutable_new_row());
  if (old_row) spectrum_row_fill_fields(table, old_row, request.mutable_old_row());

  grpc::Status status = get_storage_replica_client()->ReplicateRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_log_replicate_row[%s:%s:%d]: error=%s", table->s->db.str, table->s->table_name.str, table->file, status.error_message().c_str());
    return 1;
  }
  return 0;
}

int spectrum_log_commit(THD *thd, bool all, bool ignore_global_read_lock) {
  spectrum::CommitRequest request;
  spectrum::CommitResponse response;

  sql_print_information("spectrum_log_commit: all=%d, ignore_global_read_lock=%d", all, ignore_global_read_lock);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_all(all);
  request.set_ignore_global_read_lock(ignore_global_read_lock);

  {
    grpc::ClientContext context;
    grpc::Status status = get_storage_replica_client()->Commit(&context, request, &response);
    if (!status.ok()) {
      sql_print_error("spectrum_log_commit: error=%s", status.error_message().c_str());
      return 1;
    }
  }
  return 0;
}