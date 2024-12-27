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

bool spectrum_debug = false;

std::unique_ptr<spectrum::StorageNode::Stub> storage_client;
spectrum::StorageNode::Stub* get_storage_client() {
  if (!storage_client) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:64000", grpc::InsecureChannelCredentials());
    storage_client = spectrum::StorageNode::NewStub(channel);
  }
  return storage_client.get();
}

std::unique_ptr<spectrum::StorageNode::Stub> storage_replica_client;
spectrum::StorageNode::Stub* get_storage_replica_client() {
  if (!storage_replica_client) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:64001", grpc::InsecureChannelCredentials());
    storage_replica_client = spectrum::StorageNode::NewStub(channel);
  }
  return storage_replica_client.get();
}

void spectrum_thread_fill_system_variables(THD *thd, spectrum::Thread *spectrum_thread) {
  spectrum_thread->mutable_system_variables()->set_option_bits(thd->variables.option_bits);
}

void spectrum_thread_fill(THD *thd, spectrum::Thread *spectrum_thread) {
  spectrum_thread->set_id(thd->spectrum_thread_id);

  spectrum_thread_fill_system_variables(thd, spectrum_thread);
}

int spectrum_compute_create_table(THD *thd, TABLE *table) {
  spectrum::CreateTableRequest request;
  spectrum::CreateTableResponse response;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }
  
  sql_print_information("spectrum_create_table[%s:%d]: satrt", table->s->table_name.str, table->file);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);

  {
    grpc::ClientContext context;
    grpc::Status status = get_storage_client()->CreateTable(&context, request, &response);
    if (!status.ok()) {
      sql_print_error("spectrum_create_table[%s:%d]: error=%s",
          request.table().c_str(), table->file, status.error_message().c_str());
      assert(false);
    }
  }

  {
    grpc::ClientContext context;
    grpc::Status status = get_storage_replica_client()->CreateTable(&context, request, &response);
    if (!status.ok()) {
      sql_print_error("spectrum_create_table_replica[%s:%d]: error=%s",
          request.table().c_str(), table->file, status.error_message().c_str());
    }
  }
  return 0;
}

int spectrum_compute_lock_table(THD *thd, TABLE *table) {
  spectrum::LockTableRequest request;
  spectrum::LockTableResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_lock_table[%s:%d]: lock_type=%d", table->s->table_name.str, table->file, table->reginfo.lock_type);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);

  grpc::Status status = get_storage_client()->LockTable(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_lock_table[%s:%d]: error=%s",
        request.table().c_str(), table->file, status.error_message().c_str());
    assert(false);
  }
  return 0;
}

int spectrum_compute_unlock_table(THD *thd, TABLE *table) {
  spectrum::UnlockTableRequest request;
  spectrum::UnlockTableResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_unlock_table[%s:%d]", table->s->table_name.str, table->file);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);

  grpc::Status status = get_storage_client()->UnlockTable(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_unlock_table[%s:%d]: error=%s",
        request.table().c_str(), table->file, status.error_message().c_str());
    assert(false);
  }
  return 0;
}

int spectrum_compute_init_index(THD *thd, TABLE *table, uint index) {
  spectrum::InitIndexRequest request;
  spectrum::InitIndexResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_init_index[%s:%d]: index=%d", table->s->table_name.str, table->file, index);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);
  request.set_index(index);

  grpc::Status status = get_storage_client()->InitIndex(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_init_index[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }
  return 0;
}

int spectrum_compute_init_rnd(THD *thd, TABLE *table, bool scan) {
  spectrum::InitRndRequest request;
  spectrum::InitRndResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_init_rnd[%s:%d]: scan=%d", table->s->table_name.str, table->file, scan);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);
  request.set_scan(scan);

  grpc::Status status = get_storage_client()->InitRnd(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_init_rnd[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }
  return 0;
}

int spectrum_compute_end_index(THD *thd, TABLE *table) {
  spectrum::EndIndexRequest request;
  spectrum::EndIndexResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_end_index[%s:%d]", table->s->table_name.str, table->file);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);

  grpc::Status status = get_storage_client()->EndIndex(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_end_index[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }
  return 0;
}

int spectrum_compute_end_rnd(THD *thd, TABLE *table) {
  spectrum::EndRndRequest request;
  spectrum::EndRndResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_end_rnd[%s:%d]", table->s->table_name.str, table->file);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);

  grpc::Status status = get_storage_client()->EndRnd(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_end_rnd[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }
  return 0;
}

int spectrum_compute_read_row(THD *thd, TABLE *table, uint index,
    uchar *buf,                                               
    const uchar *key_ptr,           
    uint key_len,                    
    enum ha_rkey_function find_flag) 
{
  spectrum::ReadRowRequest request;
  spectrum::ReadRowResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  assert(buf == table->record[0]);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);
  request.set_index(index);
  request.mutable_key()->assign((const char *)key_ptr, key_len);
  request.set_key_len(key_len);
  request.set_find_flag(find_flag);

  grpc::Status status = get_storage_client()->ReadRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_read_row[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }

  if (response.has_row()) {
    spectrum::Row spectrum_row = response.row();
    spectrum_row_extract_fields(table, &spectrum_row);
    spectrum_print_row("spectrum_read_row", table);
    return 0;
  }
  sql_print_information("spectrum_read_row[%s:%d]: record not found", table->s->table_name.str, table->file);
  return HA_ERR_KEY_NOT_FOUND;
}

int spectrum_compute_read_next_row(THD *thd, TABLE *table, uint index, uchar *buf, bool same)
{
  spectrum::ReadNextRowRequest request;
  spectrum::ReadNextRowResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  assert(buf == table->record[0]);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);
  request.set_index(index);
  request.set_same(same);

  grpc::Status status = get_storage_client()->ReadNextRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_read_next_row[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }

  if (response.has_row()) {
    spectrum::Row spectrum_row = response.row();
    spectrum_row_extract_fields(table, &spectrum_row);
    spectrum_print_row("spectrum_read_next_row", table);
    return 0;
  }
  sql_print_information("spectrum_read_next_row[%s:%d]: record not found", table->s->table_name.str, table->file);
  return HA_ERR_END_OF_FILE;
}

int spectrum_compute_read_prev_row(THD *thd, TABLE *table, uint index, uchar *buf)
{
  spectrum::ReadPrevRowRequest request;
  spectrum::ReadPrevRowResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  assert(buf == table->record[0]);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);
  request.set_index(index);

  grpc::Status status = get_storage_client()->ReadPrevRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_read_prev_row[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }

  if (response.has_row()) {
    spectrum::Row spectrum_row = response.row();
    spectrum_row_extract_fields(table, &spectrum_row);
    spectrum_print_row("spectrum_read_prev_row", table);
    return 0;
  }
  sql_print_information("spectrum_read_prev_row[%s:%d]: record not found", table->s->table_name.str, table->file);
  return HA_ERR_END_OF_FILE;
}

int spectrum_compute_replicate_row(THD *thd, TABLE *table, uchar *new_row, uchar *old_row) {
  spectrum::ReplicateRowRequest request;
  spectrum::ReplicateRowResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  spectrum_print_row("spectrum_replicate_row", table);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);

  if (new_row) spectrum_row_fill_fields(table, new_row, request.mutable_new_row());
  if (old_row) spectrum_row_fill_fields(table, old_row, request.mutable_old_row());

  grpc::Status status = get_storage_replica_client()->ReplicateRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_replicate_row[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    return 1;
  }
  return 0;
}

int spectrum_compute_write_row(THD *thd, TABLE *table, uchar *record) {
  spectrum::WriteRowRequest request;
  spectrum::WriteRowResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  assert(record == table->record[0]);

  spectrum_print_row("spectrum_write_row", table);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  spectrum_row_fill_fields(table, request.mutable_row());
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);
  request.set_autoinc_field_has_explicit_non_null_value(table->autoinc_field_has_explicit_non_null_value);

  grpc::Status status = get_storage_client()->WriteRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_write_row[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }
  spectrum_row_extract_fields(table, (spectrum::Row *)&response.row());
  spectrum_print_row("spectrum_write_row_new", table);

  spectrum_compute_replicate_row(thd, table, record, nullptr);

  table->file->insert_id_for_cur_row = response.insert_id();
  return 0;
}

int spectrum_compute_update_row(THD *thd, TABLE *table, const uchar *old_record, uchar *new_record) {
  spectrum::UpdateRowRequest request;
  spectrum::UpdateRowResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  assert(new_record == table->record[0]);
  assert(old_record == table->record[1]);

  spectrum_print_row("spectrum_update_row_new", table, new_record);
  spectrum_print_row("spectrum_update_row_old", table, (uchar *)old_record);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);
  request.set_autoinc_field_has_explicit_non_null_value(table->autoinc_field_has_explicit_non_null_value);
  
  spectrum_row_fill_fields(table, table->record[0], request.mutable_new_row());
  spectrum_row_fill_fields(table, table->record[1], request.mutable_old_row());

  grpc::Status status = get_storage_client()->UpdateRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_update_row[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }

  spectrum_compute_replicate_row(thd, table, new_record, (uchar *)old_record);

  return 0;
}

int spectrum_compute_delete_row(THD *thd, TABLE *table, const uchar *record) {
  spectrum::DeleteRowRequest request;
  spectrum::DeleteRowResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  spectrum_print_row("spectrum_delete_row", table, (uchar *)record);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  spectrum_row_fill_fields(table, (uchar *)record, request.mutable_row());
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  request.set_handler((uint64)table->file);
  request.set_lock_type(table->reginfo.lock_type);

  grpc::Status status = get_storage_client()->DeleteRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_delete_row[%s:%d]: error=%s", table->s->table_name.str, table->file, status.error_message().c_str());
    assert(false);
  }

  spectrum_compute_replicate_row(thd, table, nullptr, (uchar *)record);

  return 0;
}

int spectrum_compute_commit(THD *thd, bool all, bool ignore_global_read_lock) {
  spectrum::CommitRequest request;
  spectrum::CommitResponse response;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_commit: all=%d, ignore_global_read_lock=%d", all, ignore_global_read_lock);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_all(all);
  request.set_ignore_global_read_lock(ignore_global_read_lock);

  {
    grpc::ClientContext context;
    grpc::Status status = get_storage_client()->Commit(&context, request, &response);
    if (!status.ok()) {
      sql_print_error("spectrum_commit: error=%s", status.error_message().c_str());
      assert(false);
    }
  }

  {
    grpc::ClientContext context;
    grpc::Status status = get_storage_replica_client()->Commit(&context, request, &response);
    if (!status.ok()) {
      sql_print_error("spectrum_commit_replica: error=%s", status.error_message().c_str());
    }
  }
  return 0;
}

int spectrum_compute_begin_attachable_transaction(THD *thd, bool readonly) {
  spectrum::BeginAttachableTransactionRequest request;
  spectrum::BeginAttachableTransactionResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_begin_attachable_transaction: readonly=%d", readonly);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_readonly(readonly);

  grpc::Status status = get_storage_client()->BeginAttachableTransaction(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_begin_attachable_transaction: error=%s", status.error_message().c_str());
    assert(false);
  }
  return 0;
}

int spectrum_compute_end_attachable_transaction(THD *thd) {
  spectrum::EndAttachableTransactionRequest request;
  spectrum::EndAttachableTransactionResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_end_attachable_transaction");

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);

  grpc::Status status = get_storage_client()->EndAttachableTransaction(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_end_attachable_transaction: error=%s", status.error_message().c_str());
    assert(false);
  }
  return 0;
}

int spectrum_compute_acquire_mdl(THD *thd, MDL_ticket *ticket) {
  spectrum::AcquireMetadataLockRequest request;
  spectrum::AcquireMetadataLockResponse response;
  grpc::ClientContext context;
  const MDL_key *mdl_key = ticket->get_key();
  enum_mdl_duration duration = ticket->get_duration();
  enum_mdl_type type = ticket->get_type();
  int32 ticket_number = ticket->get_ctx()->next_ticket_number++;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_compute_acquire_mdl: namespace=%d, db=%s, table=%s, column=%s, type=%d, duration=%d, ticket_number=%d",
      mdl_key->mdl_namespace(), mdl_key->db_name(), mdl_key->name(), mdl_key->col_name(), type, duration, ticket_number);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_namespace_(mdl_key->mdl_namespace());
  if (mdl_key->db_name()) request.set_schema(mdl_key->db_name());
  if (mdl_key->name()) request.set_table(mdl_key->name());
  if (mdl_key->col_name()) request.set_column(mdl_key->col_name());
  request.set_type(type);
  request.set_duration(duration);
  request.set_ticket_number(ticket_number);

  grpc::Status status = get_storage_client()->AcquireMetadataLock(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_compute_acquire_mdl: error=%s", status.error_message().c_str());
    assert(false);
  }

  ticket->ticket_number = ticket_number;
  return 0;
}

int spectrum_compute_release_mdl(THD *thd, enum_mdl_duration duration, int32 ticket_number) {
  spectrum::ReleaseMetadataLockRequest request;
  spectrum::ReleaseMetadataLockResponse response;
  grpc::ClientContext context;

  if (thd->spectrum_compute_disabled) {
    return 0;
  }

  sql_print_information("spectrum_compute_release_mdl: duration=%d, ticket_number=%d",
      duration, ticket_number);

  spectrum::Thread *spectrum_thread = request.mutable_thread();
  spectrum_thread_fill(thd, spectrum_thread);
  request.set_duration(duration);
  request.set_ticket_number(ticket_number);

  grpc::Status status = get_storage_client()->ReleaseMetadataLock(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_compute_release_mdl: error=%s", status.error_message().c_str());
    assert(false);
  }
  return 0;
}