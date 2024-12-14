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

#include <sql/field.h>
#include <sql/table.h>
#include <sql/log.h>

#include <grpc/grpc.h>
#include "spectrum.h"

void spectrum_print_row(char* method, TABLE* table) {
  spectrum_print_row(method, table, table->record[0]);
}

void spectrum_print_row(char* method, TABLE* table, uchar* record) {
  std::string row;
  char value_buffer[1024];
  String value(value_buffer, sizeof(value_buffer), &my_charset_bin);
  uint64 hander_id = table->file->spectrum_handler_id ? table->file->spectrum_handler_id : (uint64)table->file;
  MY_BITMAP *temp_read_set;

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
  sql_print_information("%s[%s:%d]: %s", method, table->s->table_name.str, hander_id, row.c_str());

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