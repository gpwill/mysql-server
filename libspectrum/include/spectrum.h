/*
   Copyright (c) 2016, 2023, Oracle and/or its affiliates.
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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef ___SPECTRUM_INCLUDED___
#define ___SPECTRUM_INCLUDED___

#include<stdlib.h>

#include "spectrum.grpc.pb.h"

extern bool spectrum_debug;

inline bool is_spectrum_compute() {
  char* env = getenv("SPECTRUM_COMPUTE_NODE");
  if (env != nullptr) {
    return true;
  }
  return false;
}

inline bool is_spectrum_storage() {
  char* env = getenv("SPECTRUM_STORAGE_NODE");
  if (env != nullptr) {
    return true;
  }
  return false;
}

extern void spectrum_print_row(char* method, TABLE* table);
extern void spectrum_print_row(char* method, TABLE* table, uchar* record);
extern void spectrum_row_fill_fields(TABLE* table, ::spectrum::Row *spectrum_row);
extern void spectrum_row_fill_fields(TABLE* table, uchar* record, ::spectrum::Row *spectrum_row);
extern void spectrum_row_extract_fields(TABLE *table, ::spectrum::Row *spectrum_row);
extern void spectrum_row_extract_fields(TABLE *table, uchar* record, ::spectrum::Row *spectrum_row);

extern int spectrum_compute_create_table(THD *thd, TABLE *table);
extern int spectrum_compute_lock_table(THD *thd, TABLE *table);
extern int spectrum_compute_unlock_table(THD *thd, TABLE *table);
extern int spectrum_compute_init_index(THD *thd, TABLE *table, uint index);
extern int spectrum_compute_init_rnd(THD *thd, TABLE *table, bool scan);
extern int spectrum_compute_end_index(THD *thd, TABLE *table);
extern int spectrum_compute_end_rnd(THD *thd, TABLE *table);
extern int spectrum_compute_read_row(THD *thd, TABLE *table, uint index, uchar *buf,
                                     const uchar *key_ptr, uint key_len, enum ha_rkey_function find_flag);
extern int spectrum_compute_read_next_row(THD *thd, TABLE *table, uint index, uchar *buf, bool same);
extern int spectrum_compute_read_prev_row(THD *thd, TABLE *table, uint index, uchar *buf);
extern int spectrum_compute_write_row(THD *thd, TABLE *table, uchar *record);
extern int spectrum_compute_update_row(THD *thd, TABLE *table, const uchar *old_record, uchar *new_record);
extern int spectrum_compute_delete_row(THD *thd, TABLE *table, const uchar *record);
extern int spectrum_compute_commit(THD *thd, bool all, bool ignore_global_read_lock);
extern int spectrum_compute_begin_attachable_transaction(THD *thd, bool readonly);
extern int spectrum_compute_end_attachable_transaction(THD *thd);

#endif