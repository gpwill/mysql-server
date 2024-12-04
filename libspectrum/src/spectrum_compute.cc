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

#include "spectrum.h"
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include "spectrum.grpc.pb.h"

void spectrum_thread_fill_mdl_list_for_duration(THD *thd, spectrum::Thread *spectrum_thread, enum_mdl_duration duration) {
  MDL_context::Ticket_iterator it = thd->mdl_context.get_tickets_for_duration(duration);
  for (MDL_ticket *t = it++; t != nullptr; t = it++) {
    sql_print_information("MDL ticket: namespace=%d, db=%s, name=%s, column=%s, type=%d, duration=%d",
      t->get_key()->mdl_namespace(), t->get_key()->db_name(), t->get_key()->name(), t->get_key()->col_name(), t->get_type(), duration);

    spectrum::MDL *spectrum_mdl = spectrum_thread->add_mdl_list();
    if (t->get_key()->mdl_namespace()) {
      spectrum_mdl->set_namespace_(t->get_key()->mdl_namespace());
    }
    if (t->get_key()->db_name()) {
      spectrum_mdl->set_schema(t->get_key()->db_name());
    }
    if (t->get_key()->name()) {
      spectrum_mdl->set_table(t->get_key()->name());
    }
    if (t->get_key()->col_name()) {
      spectrum_mdl->set_column(t->get_key()->col_name());
    }
    if (t->get_type()) {
      spectrum_mdl->set_type(t->get_type());
    }
    spectrum_mdl->set_duration(duration);
  }
}

void spectrum_thread_fill_mdl_list(THD *thd, spectrum::Thread *spectrum_thread) {
  spectrum_thread_fill_mdl_list_for_duration(thd, spectrum_thread, enum_mdl_duration::MDL_TRANSACTION);
  spectrum_thread_fill_mdl_list_for_duration(thd, spectrum_thread, enum_mdl_duration::MDL_STATEMENT);
}

void spectrum_row_fill_record(TABLE* table, spectrum::Row *spectrum_row) {
  char value_buffer[1024];
  String value(value_buffer, sizeof(value_buffer), &my_charset_bin);

  for (Field **field = table->field; *field; field++) {
    spectrum::Field *spectrum_field = spectrum_row->add_fields();
    spectrum_field->set_name((*field)->field_name);
    if (!(*field)->is_null()) {
      (*field)->val_str(&value, &value);
      spectrum_field->set_value(value.c_ptr());
    } else {
      spectrum_field->set_is_null(true);
    }
  }
}

int spectrum_compute_create_table(THD *thd, TABLE *table) {
  spectrum::CreateTableRequest request;
  spectrum::CreateTableResponse response;
  grpc::ClientContext context;

  if (!is_spectrum_compute_node()) {
    return 1;
  }

  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:64000", grpc::InsecureChannelCredentials());
  std::unique_ptr<spectrum::StorageNode::Stub> storage_node_stub = spectrum::StorageNode::NewStub(channel);

  spectrum_thread_fill_mdl_list(thd, request.mutable_thread());
  request.set_database(table->s->db.str);
  request.set_table(table->s->table_name.str);
  grpc::Status status = storage_node_stub.get()->CreateTable(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_create_table[%s:%s]: error=%s",
        request.database().c_str(), request.table().c_str(), status.error_message().c_str());
  }
  return 0;
}

int spectrum_compute_write_row(THD *thd, TABLE *table, uchar *record) {
  spectrum::WriteRowRequest request;
  spectrum::WriteRowResponse response;
  grpc::ClientContext context;

  if (!is_spectrum_compute_node()) {
    return 1;
  }

  spectrum_print_row("spectrum_write_row", table);

  request.set_table(table->s->table_name.str);
  request.set_database(table->s->db.str);
  request.set_autoinc_field_has_explicit_non_null_value(table->autoinc_field_has_explicit_non_null_value);
  spectrum_row_fill_record(table, request.mutable_row());
  spectrum_thread_fill_mdl_list(thd, request.mutable_thread());

  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:64000", grpc::InsecureChannelCredentials());
  std::unique_ptr<spectrum::StorageNode::Stub> storage_node_stub = spectrum::StorageNode::NewStub(channel);
  grpc::Status status = storage_node_stub.get()->WriteRow(&context, request, &response);
  if (!status.ok()) {
    sql_print_error("spectrum_write_row[%s]: error=%s", table->s->table_name.str, status.error_message().c_str());
    return 2;
  }

  table->file->insert_id_for_cur_row = response.insert_id();
  return 0;
}

int spectrum_compute_update_row(THD *thd, TABLE *table, const uchar *old_record, uchar *new_record) {
  spectrum::UpdateRowRequest request;
  spectrum::UpdateRowResponse response;
  grpc::ClientContext context;

  if (!is_spectrum_compute_node()) {
    return 1;
  }

  spectrum_print_row("spectrum_update_row", table);

  request.set_table(table->s->table_name.str);
  request.set_database(table->s->db.str);
  request.set_autoinc_field_has_explicit_non_null_value(table->autoinc_field_has_explicit_non_null_value);
  spectrum_thread_fill_mdl_list(thd, request.mutable_thread());
  
  spectrum_row_fill_record(table, request.mutable_new_row());
  restore_record(table, record[1]);
  spectrum_row_fill_record(table, request.mutable_old_row());

  //std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:64000", grpc::InsecureChannelCredentials());
  //std::unique_ptr<spectrum::StorageNode::Stub> storage_node_stub = spectrum::StorageNode::NewStub(channel);
  //grpc::Status status = storage_node_stub.get()->UpdateRow(&context, request, &response);
  //if (!status.ok()) {
  //  sql_print_error("spectrum_update_row[%s]: error=%s", table->s->table_name.str, status.error_message().c_str());
  //  return 2;
  //}
  return 0;
}
