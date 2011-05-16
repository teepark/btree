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

#ifndef SORTEDBTREE_H
#define SORTEDBTREE_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * leaf and branch nodes, and a generic node_t to which they are both castable
 */
#define NODE_HEAD      \
    int filled;        \
    PyObject **values; \

typedef struct btree_leaf {
    NODE_HEAD
} leaf_t;

typedef leaf_t node_t;

typedef struct btree_branch {
    NODE_HEAD
    node_t **children;
} branch_t;


/*
 * the PyObject that wraps a root node and gets exposed to python
 */
typedef struct {
    PyObject_HEAD
    int order;
    int depth;
    char flags;
    node_t *root;
} sorted_btree_object;

/*
 * the PyTypeObject of the btree class
 */
PyTypeObject sorted_btree_type;

#define SORTBTREE_FLAG_INITED 1


/*
 * the necessary info to save our position and be
 * able to pick up traversing the tree in order
 */
typedef struct {
    int depth;
    int *indexes;
    node_t **lineage;
    sorted_btree_object *tree;
} path_t;

#define PYBTREE_STACK_ALLOC_PATH(treeobj)   \
    path_t path;                            \
    int _indexes[(treeobj)->depth + 1];     \
    node_t *_lineage[(treeobj)->depth + 1]; \
    path.tree = (treeobj);                  \
    path.indexes = _indexes;                \
    path.lineage = _lineage;


/*
 * the PyObject of a sorted_btree iterator
 */
typedef struct {
    PyObject_HEAD

    path_t *path;
} sorted_btree_iterator;


/*
 * visitor function signatures for handing off to traversers
 */
typedef int (*nodevisitor)(
        node_t *node, char is_branch, int depth, void *data);

typedef int (*itemvisitor)(
        PyObject *item, char is_branch, int depth, void *data);


/*
 * C api functions
 */
int py_sorted_btree_insert(PyObject *tree, PyObject *item);
int py_sorted_btree_remove(PyObject *tree, PyObject *item);

#ifdef __cplusplus
}
#endif

#endif /* SORTEDBTREE_H */
