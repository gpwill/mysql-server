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

#include "sql/table.h"
#include "sql/dd/object_id.h"
#include "sql/dd/types/table.h"
#include "sql/dd/types/schema.h"

#include "spectrum.grpc.pb.h"

namespace spectrum {
enum event_type_enum {
   CREATE_TABLE,
   DELETE_TABLE,
   POST_DDL,
   UPDATE_METADATA,
   ADD_ROW,
   PREPARE,
   COMMIT
};
}

typedef uint64 event_id_t;
typedef uint64 commit_id_t;

namespace spectrum_storage {
class THD_context {
  private:
    THD *m_thd;
    query_id_t m_compute_query_id;
    event_id_t m_event_id;
    commit_id_t m_commit_id;
    uint64 m_replication_stream_id;
    bool m_post_ddl;

  public:
    THD_context(THD *thd) : m_thd(thd), m_compute_query_id(0), m_event_id(0), m_commit_id(0), m_replication_stream_id(0), m_post_ddl(false) {}

    query_id_t compute_query_id() {
      return m_compute_query_id;
    }

    void set_compute_query_id(query_id_t compute_query_id) {
      m_compute_query_id = compute_query_id;
    }

    commit_id_t commit_id() {
      return m_commit_id;
    }

    void set_commit_id(commit_id_t commit_id) {
      m_commit_id = commit_id;
    }

    void clear_commit_id() {
      m_commit_id = 0;
    }

    uint64 replication_stream_id() {
      return m_replication_stream_id;
    }

    void set_replication_stream_id(uint64 replication_stream_id) {
      m_replication_stream_id = replication_stream_id;
    }

    bool post_ddl() {
      return m_post_ddl;
    }

    void set_post_ddl(bool post_ddl) {
      m_post_ddl = post_ddl;
    }

    my_xid xid();
    event_id_t next_event_id();
};
}

extern bool spectrum_debug;

extern bool is_spectrum_compute();
extern bool is_spectrum_storage();
extern bool is_spectrum_storage_primary();
extern bool is_spectrum_storage_replica();

extern void disable_spectrum_compute(THD *thd);
extern void enable_spectrum_compute(THD *thd);

extern spectrum::StorageNode::Stub* get_storage_primary_client();

extern void spectrum_print_row(char* method, TABLE* table);
extern void spectrum_print_row(char* method, TABLE* table, uchar* record);
extern void spectrum_row_fill_fields(TABLE* table, ::spectrum::Row *spectrum_row);
extern void spectrum_row_fill_fields(TABLE* table, uchar* record, ::spectrum::Row *spectrum_row);
extern void spectrum_row_extract_fields(TABLE *table, ::spectrum::Row *spectrum_row);
extern void spectrum_row_extract_fields(TABLE *table, uchar* record, ::spectrum::Row *spectrum_row);
extern void spectrum_thread_fill(THD *thd, spectrum::Thread *spectrum_thread);

TABLE *spectrum_open_table(THD *thd, const char *db_name, const char *table_name, uint64 handler_id, thr_lock_type lock_type, thr_locked_row_action lock_action);
TABLE *spectrum_find_or_open_table(THD *thd, const char *db_name, const char *table_name, uint64 handler_id, thr_lock_type lock_type, thr_locked_row_action lock_action);
TABLE *spectrum_find_or_open_table(THD *thd, const char *db_name, const char *table_name, thr_lock_type lock_type, thr_locked_row_action lock_action);
void spectrum_find_and_close_table(THD *thd, const char *db_name, const char *table_name,uint64 handler_id);

template<typename T>
int spectrum_compute_update_metadata(THD *thd, const T *object);
extern int spectrum_compute_create_table(THD *thd, TABLE *table);
extern int spectrum_compute_delete_table(THD *thd, const dd::Table *table_def, const char* table_path);
extern int spectrum_compute_lock_table(THD *thd, TABLE *table);
extern int spectrum_compute_unlock_table(THD *thd, TABLE *table);
extern int spectrum_compute_close_table(THD *thd, TABLE *table);
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
extern int spectrum_compute_prepare(THD *thd, bool all);
extern int spectrum_compute_commit(THD *thd, bool all);
extern int spectrum_compute_begin_attachable_transaction(THD *thd, bool readonly);
extern int spectrum_compute_end_attachable_transaction(THD *thd);
extern int spectrum_compute_acquire_mdl(THD *thd, MDL_ticket *ticket);
extern int spectrum_compute_release_mdl(THD *thd, enum_mdl_duration duration, int32 ticket_number);
extern int spectrum_compute_post_ddl(THD *thd);

extern void* spectrum_storage_init(void *);

extern int spectrum_log_init(THD *thd);
extern commit_id_t spectrum_log_max_commit_id();
extern int spectrum_log_create_table(THD *thd, const char* db_name, const char* table_name, uint64 handler_id);
extern int spectrum_log_delete_table(THD *thd, const char* db_name, const char* table_name, const char* table_path);
extern int spectrum_log_post_ddl(THD *thd);
extern int spectrum_log_update_metadata(THD *thd, const char* table, dd::Object_id object_id, const char* object_name);
extern int spectrum_log_add_row(THD *thd, TABLE *table, uchar *new_row, uchar *old_row);
extern int spectrum_log_prepare(THD *thd, bool all, bool real_trans);
extern int spectrum_log_commit(THD *thd, bool all, bool real_trans);
extern int spectrum_log_write_commit(THD *thd, commit_id_t commit_id, my_xid xid);
extern int spectrum_log_read_commit(THD *thd, uint64 start_id_exclusive, spectrum::Commit *commit);
extern int spectrum_log_write_event(THD *thd, spectrum::Event *event);
extern int spectrum_log_read_events_by_xid(THD *thd, my_xid xid, spectrum::EventList *events);
extern int spectrum_log_read_last_event(THD *thd, spectrum::Event *event);

#endif