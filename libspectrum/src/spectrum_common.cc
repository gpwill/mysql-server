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


#include <string>

#include <sql/sql_class.h>
#include <sql/field.h>
#include <sql/table.h>
#include <sql/log.h>
#include <sql/sql_base.h>

#include <mysql/plugin.h>

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include "spectrum.h"
#include "spectrum_config.h"

bool is_spectrum_compute() {
  node_config_t *node_config = find_current_node_config();
  if (!node_config) return false;
  return node_config->role == node_role_enum::COMPUTE;
}

bool is_spectrum_storage() {
  node_config_t *node_config = find_current_node_config();
  if (!node_config) return false;
  return node_config->role == node_role_enum::STORAGE_PRIMARY || node_config->role == node_role_enum::STORAGE_REPLICA;
}

bool is_spectrum_storage_primary() {
  node_config_t *node_config = find_current_node_config();
  if (!node_config) return false;
  return node_config->role == node_role_enum::STORAGE_PRIMARY;
}

bool is_spectrum_storage_replica() {
  node_config_t *node_config = find_current_node_config();
  if (!node_config) return false;
  return node_config->role == node_role_enum::STORAGE_REPLICA;
}

void disable_spectrum_compute(THD *thd) {
  thd->spectrum_compute_disabled = true;
}

void enable_spectrum_compute(THD *thd) {
  thd->spectrum_compute_disabled = false;
}

std::unique_ptr<spectrum::StorageNode::Stub> storage_primary_client;
spectrum::StorageNode::Stub* get_storage_primary_client() {
  if (!storage_primary_client) {
    node_config_t* node_config = find_storage_primary_node_config();
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(node_config->address, grpc::InsecureChannelCredentials());
    storage_primary_client = spectrum::StorageNode::NewStub(channel);
  }
  return storage_primary_client.get();
}

void spectrum_thread_fill_system_variables(THD *thd, spectrum::Thread *spectrum_thread) {
  spectrum_thread->mutable_system_variables()->set_option_bits(thd->variables.option_bits);
}

void spectrum_thread_fill(THD *thd, spectrum::Thread *spectrum_thread) {
  spectrum_thread->set_id(thd->spectrum_thread_id);
  spectrum_thread->set_tx_isolation(thd->tx_isolation);
  spectrum_thread->set_query_id(thd->query_id);
  spectrum_thread->set_sql_command(thd_sql_command(thd));

  spectrum_thread_fill_system_variables(thd, spectrum_thread);
}

void spectrum_print_row(char* method, TABLE* table) {
  spectrum_print_row(method, table, table->record[0]);
}

void spectrum_print_row(char* method, TABLE* table, uchar* record) {
  std::string row;
  char value_buffer[1024];
  String value(value_buffer, sizeof(value_buffer), &my_charset_bin);
  uint64 hander_id = table->file->spectrum_handler_id ? table->file->spectrum_handler_id : (uint64)table->file;
  MY_BITMAP *temp_read_set;

  if (!record) {
    sql_print_information("%s[%s:%s:%d]: null", method, table->s->db.str, table->s->table_name.str, hander_id);
    return;
  }

  temp_read_set = table->read_set;
  table->read_set = nullptr;
  repoint_field_to_record(table, table->record[0], record);

  for (Field **field = table->field; *field; field++) {
    row += (*field)->field_name;
    row += '=';
    if (!(*field)->is_null()) {
      value.set_charset((*field)->charset());
      (*field)->val_str(&value, &value);
      row += value.c_ptr();
    }
    row += ", ";
  }
  if (row.length() >= 2) {
    row.pop_back();
    row.pop_back();
  }
  sql_print_information("%s[%s:%s:%d]: %s", method, table->s->db.str, table->s->table_name.str, hander_id, row.c_str());

  repoint_field_to_record(table, record, table->record[0]);
  table->read_set = temp_read_set;
}

void spectrum_row_fill_fields(TABLE* table, spectrum::Row *spectrum_row) {
  spectrum_row_fill_fields(table, table->record[0], spectrum_row);
}

void spectrum_row_fill_fields(TABLE* table, uchar* record, spectrum::Row *spectrum_row) {
  char value_buffer[1024];
  String value(value_buffer, sizeof(value_buffer), &my_charset_bin);

  repoint_field_to_record(table, table->record[0], record);

  for (Field **field = table->field; *field; field++) {
    spectrum::Field *spectrum_field = spectrum_row->add_fields();
    spectrum_field->set_name((*field)->field_name);
    if (!(*field)->is_null()) {
      value.set_charset((*field)->charset());
      (*field)->val_str(&value, &value);
      spectrum_field->set_value(value.c_ptr(), value.length());
    } else {
      spectrum_field->set_is_null(true);
    }
  }

  repoint_field_to_record(table, record, table->record[0]);
}

void spectrum_row_extract_fields(TABLE *table, spectrum::Row *spectrum_row) {
  spectrum_row_extract_fields(table, table->record[0], spectrum_row);
}

void spectrum_row_extract_fields(TABLE *table, uchar* record, spectrum::Row *spectrum_row) {
  MY_BITMAP *temp_write_set;

  temp_write_set = table->write_set;
  table->write_set = nullptr;
  repoint_field_to_record(table, table->record[0], record);

  memset(record, 0, table->s->null_bytes);
  for (int i = 0; i < spectrum_row->fields().size(); i++) {
    Field *field = table->field[i];
    ::spectrum::Field spectrum_field = spectrum_row->fields()[i];

    assert(!strcmp(field->field_name, spectrum_field.name().c_str()));

    if (!spectrum_field.is_null()) {
      std::string value = spectrum_field.value();
      field->store(value.data(), value.length(), field->charset());
    } else {
      field->set_null();
    }
  }

  table->write_set = temp_write_set;
  repoint_field_to_record(table, record, table->record[0]);
}

TABLE *spectrum_open_table(
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

TABLE *spectrum_find_or_open_table(
    THD *thd,
    const char *db_name,
    const char *table_name,
    uint64 handler_id,
    thr_lock_type lock_type,
    thr_locked_row_action lock_action)
{
  for (TABLE *t = thd->open_tables; t; t = t->next) {
    if ((!handler_id || t->file->spectrum_handler_id == handler_id) &&
        !strcmp(t->s->db.str, db_name) &&
        !strcmp(t->s->table_name.str, table_name))
      return t;
  }
  return spectrum_open_table(thd, db_name, table_name, handler_id, lock_type, lock_action);
}

TABLE *spectrum_find_or_open_table(
    THD *thd,
    const char *db_name,
    const char *table_name,
    thr_lock_type lock_type,
    thr_locked_row_action lock_action)
{
  return spectrum_find_or_open_table(thd, db_name, table_name, 0, lock_type, lock_action);
}

void spectrum_find_and_close_table(
    THD *thd,
    const char *db_name,
    const char *table_name,
    uint64 handler_id)
{
  TABLE **table;
  for (table = &thd->open_tables; *table; table = &(*table)->next) {
    if ((!handler_id || (*table)->file->spectrum_handler_id == handler_id) &&
        !strcmp((*table)->s->db.str, db_name) &&
        !strcmp((*table)->s->table_name.str, table_name))
      break;
  }
  if (*table) {
    close_thread_table(thd, table);
  }
}