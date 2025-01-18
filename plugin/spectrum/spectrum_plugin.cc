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

#include <mysql/plugin.h>
#include <stdio.h>
#include <stdlib.h>

#include "my_thread.h"
#include "mysql/psi/mysql_memory.h"
#include "sql/sql_plugin.h"  // st_plugin_int
#include "sql/log.h"

#include "spectrum.h"
#include "spectrum_config.h"

struct spectrum_plugin_context {
};

PSI_memory_key key_memory_spectrum_plugin_context;

static int spectrum_plugin_init(void *p) {
  DBUG_TRACE;
  struct spectrum_plugin_context *plugin_context;
  struct st_plugin_int *plugin = (struct st_plugin_int *)p;

  plugin_context = (struct spectrum_plugin_context *)my_malloc(
      key_memory_spectrum_plugin_context,
      sizeof(struct spectrum_plugin_context), MYF(0));
  plugin->data = (void *)plugin_context;

  spectrum_config_init();

  if (is_spectrum_storage()) {
    my_thread_attr_t storage_init_thread_attr;
    my_thread_attr_init(&storage_init_thread_attr);
    my_thread_attr_setdetachstate(&storage_init_thread_attr, MY_THREAD_CREATE_JOINABLE);

    my_thread_handle storage_init_thread;
    if (!my_thread_create(&storage_init_thread, &storage_init_thread_attr, spectrum_storage_init, (void *)nullptr)) {
      void *retval;
      my_thread_join(&storage_init_thread, &retval);
    } else {
      sql_print_error("Could not start spectrum storage init thread");
    }
  }
  return 0;
}

static int spectrum_plugin_deinit(void *p) {
  DBUG_TRACE;

  return 0;
}

struct st_mysql_daemon spectrum_plugin = {MYSQL_DAEMON_INTERFACE_VERSION};

/*
  Plugin library descriptor
*/
mysql_declare_plugin(spectrum){
    MYSQL_DAEMON_PLUGIN,
    &spectrum_plugin,
    "spectrum",
    PLUGIN_AUTHOR_ORACLE,
    "Spectrum",
    PLUGIN_LICENSE_GPL,
    spectrum_plugin_init,   /* Plugin Init */
    nullptr,                      /* Plugin Check uninstall */
    spectrum_plugin_deinit, /* Plugin Deinit */
    0x0100 /* 1.0 */,
    nullptr, /* status variables                */
    nullptr, /* system variables                */
    nullptr, /* config options                  */
    0,       /* flags                           */
} mysql_declare_plugin_end;