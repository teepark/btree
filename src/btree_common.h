/*
 * Copyright (c) 2009-2011, Travis Parker
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials provided
 *       with the distribution.
 *     * Neither the name of the author nor the names of other
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef BT_COMMON_H
#define BT_COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

#include "Python.h"
#include "structmember.h"

#if PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION <= 5
    #define Py_TYPE(ob) (((PyObject *)(ob))->ob_type)
#endif


/*
 * leaves, branches and a generic bt_node_t to which they are both castable
 */
#define BT_NODE_HEAD \
    int filled;      \
    PyObject **values;

typedef struct {
    BT_NODE_HEAD
} bt_node_t;
typedef bt_node_t bt_leaf_t;

#define BT_BRANCH_HEAD \
    BT_NODE_HEAD       \
    bt_node_t **children;

typedef struct {
    BT_BRANCH_HEAD
} bt_branch_t;


/*
 * a polymorphism typedef for btree python objects
 */
#define BT_PYOBJECT \
    PyObject_HEAD   \
    int order;      \
    int depth;      \
    char flags;

typedef struct {
    BT_PYOBJECT
    bt_node_t *root;
} bt_pyobject;


/*
 * flags for btree pyobjects
 */
#define BT_FLAG_INITED 1


/*
 * the necessary info to save a position and be
 * able to resume traversing a tree in order
 */
typedef struct {
    int depth;
    int *indexes;
    bt_node_t **lineage;
    bt_pyobject *tree;
} bt_path_t;

#define BT_STACK_ALLOC_PATH(treeobj)           \
    bt_path_t path;                            \
    int _indexes[(treeobj)->depth + 1];        \
    bt_node_t *_lineage[(treeobj)->depth + 1]; \
    path.tree = (treeobj);                     \
    path.indexes = _indexes;                   \
    path.lineage = _lineage;


/*
 * a python iterator object over a bt_pyobject
 */
#define BT_ITERATOR_HEAD \
    PyObject_HEAD        \
    bt_path_t *path;

typedef struct {
    BT_ITERATOR_HEAD
} bt_iter_pyobject;


/*
 * visitor function signatures for handing off to traversers
 */
typedef int (*bt_node_visitor)(
        bt_node_t *node, char is_branch, int depth, void *data);

typedef int (*bt_item_visitor)(
        PyObject *item, char is_branch, int depth, void *data);


/*
 * a printf for pyobjects for debugging
 */
#define PRINTF(format_str, py_object) do {                             \
    PyObject *__printf_pyobj = PyObject_Repr((PyObject *)(py_object)); \
    printf((format_str), PyString_AsString(__printf_pyobj));           \
    Py_DECREF(__printf_pyobj);                                         \
} while(0)


#ifdef __cplusplus
}
#endif

#endif /* BT_COMMON_H */
