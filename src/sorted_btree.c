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

#include "Python.h"
#include "structmember.h"

#include "sorted_btree.h"
#include "offsetstring.h"

#include "btree_common.h"
#include "btree_common.c"


/*
 * allocate/free a node
 */
static bt_node_t *
allocate_node(char is_branch, int order) {
    bt_node_t *node;
    if (is_branch) {
        node = malloc(sizeof(btsort_branch_t *));
        ((btsort_branch_t *)node)->children = malloc(
            sizeof(bt_node_t *) * (order + 2));
    } else
        node = malloc(sizeof(btsort_leaf_t *));
    node->values = malloc(sizeof(PyObject *) * (order + 1));
    node->filled = 0;
    return node;
}

static void
free_node(char is_branch, bt_node_t *node) {
    free(node->values);
    if (is_branch) free(((btsort_branch_t *)node)->children);
    free(node);
}


/*
 * callbacks for common_footer.h code
 */
static void
node_sizechange(bt_path_t *path) {};

static void node_pass_left(bt_path_t *path, int count) {
    btnode_pass_left(path, count);
}

static void node_pass_right(bt_path_t *path, int count) {
    btnode_pass_right(path, count);
}


/*
 * find the left- or right-most index in a PyObject array into
 * which a value is insertable still preserving sorted order
 */
static int
bisect_left(PyObject **array, int array_length, PyObject *value) {
    int cmp,
        min = 0,
        max = array_length,
        mid;
    while (min < max) {
        mid = (max + min) / 2;
        if ((cmp = PyObject_RichCompareBool(value, array[mid], Py_GT)) < 0)
            return cmp;
        if (cmp) min = mid + 1;
        else max = mid;
    }
    return min;
}

static int
bisect_right(PyObject **array, int array_length, PyObject *value) {
    int cmp,
        min = 0,
        max = array_length,
        mid;
    while (min < max) {
        mid = (max + min) / 2;
        if ((cmp = PyObject_RichCompareBool(value, array[mid], Py_GE)) < 0)
            return cmp;
        if (cmp) min = mid + 1;
        else max = mid;
    }
    return min;
}


/*
 * trace a path to the appropriate leaf for insertion
 */
static int
find_path_to_leaf(btsort_pyobject *tree, PyObject *value, char first,
        bt_path_t *path) {
    int i, index;
    bt_node_t *node = (bt_node_t *)(tree->root);
    int (*bisector)(PyObject **, int, PyObject *);

    bisector = first ? bisect_left : bisect_right;
    path->tree = tree;

    for (i = 0; i <= tree->depth; ++i) {
        if (i) node = ((bt_branch_t *)node)->children[index];

        if ((index = bisector(node->values, node->filled, value)) < 0)
            return index;

        path->lineage[i] = node;
        path->indexes[i] = index;
    }
    path->depth = tree->depth;
    return 0;
}


/*
 * trace a path to an item matching a given python value
 */
static int
find_path_to_item(btsort_pyobject *tree, PyObject *value, char first,
        char find, bt_path_t *path, char *found) {
    int i, index, cmp = 0;
    bt_node_t *node = (bt_node_t *)(tree->root);
    int (*bisector)(PyObject **, int, PyObject *);

    bisector = first ? bisect_left : bisect_right;
    if (find) *found = 0;
    path->tree = tree;

    for (i = 0; i <= tree->depth; ++i) {
        if (i) node = ((bt_branch_t *)node)->children[index];

        if ((index = bisector(node->values, node->filled, value)) < 0)
            return index;

        if (find && index < node->filled && (cmp = PyObject_RichCompareBool(
                node->values[index - (first ? 0 : 1)], value, Py_EQ)) < 0)
            return cmp;

        path->lineage[i] = node;
        path->indexes[i] = index;

        if (cmp) {
            *found = 1;
            break;
        }
    }

    path->depth = i;
    return 0;
}


/*
 * shallow copy of a sorted_btree object
 */
static bt_node_t *
copy_node(bt_node_t *node, int depth, int leaf_depth, int order) {
    int i;
    bt_node_t *result = allocate_node(depth < leaf_depth, order);

    for (i = 0; i < node->filled; ++i) {
        result->values[i] = node->values[i];
        Py_INCREF(node->values[i]);
    }
    result->filled = node->filled;

    if (depth < leaf_depth)
        for (i = 0; i <= node->filled; ++i) {
            ((bt_branch_t *)result)->children[i] = copy_node(
                ((bt_branch_t *)node)->children[i], depth + 1, leaf_depth, order);
        }

    return result;
}

static btsort_pyobject *
copy_tree(btsort_pyobject *tree) {
    btsort_pyobject *result = PyObject_GC_New(
            btsort_pyobject, &btsort_pytypeobj);
    PyObject_GC_Track(result);
    result->order = tree->order;
    result->depth = tree->depth;
    result->flags = 0;

    result->root = copy_node(tree->root, 0, tree->depth, tree->order);
    result->flags = BT_FLAG_INITED;
    return result;
}


/*
 * initializer for the python type
 */
static char *btree_init_args[] = {"order", NULL};

static int
sorted_btree_object_init(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *order;
    btsort_pyobject *tree = (btsort_pyobject *)self;

    tree->flags = 0;

    if (!PyArg_ParseTupleAndKeywords(
                args, kwargs, "O!", btree_init_args, &PyInt_Type, &order))
        return -1;

    tree->order = (int)PyInt_AsLong(order);
    tree->depth = 0;
    tree->root = allocate_node(0, tree->order);
    tree->flags |= BT_FLAG_INITED;

    if (tree->order < 2) {
        PyErr_SetString(PyExc_ValueError, "btree order must be >1");
        return -1;
    }

    return 0;
}


/*
 * python btree instance deallocator
 */
static int
dealloc_visitor(bt_node_t *node, char is_branch, int depth, void *data) {
    int i;
    for (i = 0; i < node->filled; ++i)
        Py_DECREF(node->values[i]);
    free_node(is_branch, node);
    return 0;
}

static void
sorted_btree_dealloc(btsort_pyobject *self) {
    PyObject_GC_UnTrack(self);
    if (self->flags & BT_FLAG_INITED)
        traverse_nodes(self, 0, dealloc_visitor, NULL);
    self->ob_type->tp_free((PyObject *)self);
}


/*
 * python btree GC visitor
 */
typedef struct {
    visitproc visit;
    void *arg;
} traverse_payload;

static int traverse_visitor(bt_node_t *node, char is_branch, int depth,
        void *payload) {
    int i;
    visitproc visit = ((traverse_payload *)payload)->visit;
    void *arg = ((traverse_payload *)payload)->arg;
    for (i = 0; i < node->filled; ++i) {
        Py_VISIT(node->values[i]);
    }
    return 0;
}

static int
sorted_btree_traverse(btsort_pyobject *self, visitproc visit, void *arg) {
    traverse_payload payload = {visit, arg};
    return traverse_nodes(self, 1, traverse_visitor, (void *)(&payload));
}


/*
 * python btree GC clearer
 */
static int
clear_visitor(bt_node_t *node, char is_branch, int depth, void *payload) {
    int i;
    for (i = 0; i < node->filled; ++i)
        Py_CLEAR(node->values[i]);
    return 0;
}

static int
sorted_btree_clear(btsort_pyobject *self) {
    return traverse_nodes(self, 0, clear_visitor, NULL);
}


/*
 * repr handling
 */

static int
repr_visit(bt_node_t *node, char is_branch, int depth, void *data) {
    offsetstring *string = (offsetstring *)data;
    int i, j, rc;
    PyObject *reprd;
    char item_str[OFFSETSTRING_INITIAL_SIZE];

    offsetstring_resize(string, depth * 2, &rc);
    if (rc) {
        PyErr_SetString(PyExc_MemoryError, "failed realloc");
        return rc;
    }

    for (i = 0; i < depth; ++i)
        offsetstring_write(string, "  ", 2, &rc);

    j = sprintf(item_str, "<%s filled=%d values=(",
            is_branch ? "BRANCH" : "LEAF",
            node->filled);

    for (i = 0; i < node->filled; ++i) {
        reprd = PyObject_Repr(node->values[i]);
        if (reprd == NULL) return -1;
        j += sprintf(item_str + j, "%s, ", PyString_AsString(reprd));
        Py_DECREF(reprd);
    }

    if (node->filled) j -= 2;

    offsetstring_resize(string, j + 4, &rc);
    if (rc) {
        PyErr_SetString(PyExc_MemoryError, "failed realloc");
        return rc;
    }

    offsetstring_write(string, item_str, j, &rc);
    offsetstring_write(string, ")>\n", 3, &rc);

    return 0;
}

static PyObject *
python_sorted_btree_repr(PyObject *self) {
    btsort_pyobject *tree = (btsort_pyobject *)self;
    PyObject *result;
    int rc;
    offsetstring *string;
    if ((rc = Py_ReprEnter(self))) {
        if (rc < 0) return NULL;
        return PyString_FromString("<...>");
    }

    offsetstring_new(string, &rc);
    if (rc) {
        PyErr_SetString(PyExc_MemoryError, "failed malloc");
        return NULL;
    }

    if (traverse_nodes(tree, 1, repr_visit, (void *)string))
        result = NULL;
    else
        result = PyString_FromStringAndSize(
                offsetstring_data(string), offsetstring_offset(string) - 1);

    Py_ReprLeave(self);
    offsetstring_del(string);
    return result;
}


/*
 * python insert method for btree objects
 */
static PyObject *
python_sorted_btree_insert(PyObject *self, PyObject *args) {
    PyObject *item;
    btsort_pyobject *tree = (btsort_pyobject *)self;
    BT_STACK_ALLOC_PATH(tree)

    if (!PyArg_ParseTuple(args, "O", &item)) return NULL;

    if (find_path_to_leaf(tree, item, 1, &path)) {
        return NULL;
    }

    Py_INCREF(item);
    leaf_insert(item, &path);

    Py_INCREF(Py_None);
    return Py_None;
}


/*
 * C api function for btree insert
 */
int
py_sorted_btree_insert(PyObject *tree, PyObject *item) {
    int rc;

    if (Py_TYPE(tree) != &btsort_pytypeobj) {
        PyErr_SetString(PyExc_TypeError, "sorted_btree object expected");
        return -1;
    }

    BT_STACK_ALLOC_PATH((btsort_pyobject *)tree)

    rc = find_path_to_leaf((btsort_pyobject *)tree, item, 1, &path);
    if (rc) return rc;

    Py_INCREF(item);
    leaf_insert(item, &path);
    return 0;
}


/*
 * python remove() method for deletion
 */
static PyObject *
python_sorted_btree_remove(PyObject *self, PyObject *args) {
    btsort_pyobject *tree = (btsort_pyobject *)self;
    PyObject *item;
    char found;
    BT_STACK_ALLOC_PATH(tree)

    if (!PyArg_ParseTuple(args, "O", &item)) return NULL;

    if (find_path_to_item(tree, item, 1, 1, &path, &found))
        return NULL;

    if (!found) {
        PyErr_SetString(PyExc_ValueError, "btree.remove(x): x not in btree");
        return NULL;
    }

    Py_DECREF(path.lineage[path.depth]->values[path.indexes[path.depth]]);

    if (path.depth < path.tree->depth)
        branch_removal(&path);
    else
        leaf_removal(&path);

    Py_INCREF(Py_None);
    return Py_None;
}

/*
 * C api function for btree removal
 */
int
py_sorted_btree_remove(PyObject *tree, PyObject *item) {
    int rc;
    char found;

    if (Py_TYPE(tree) != &btsort_pytypeobj) {
        PyErr_SetString(PyExc_TypeError, "sorted_btree object expeected");
        return -1;
    }

    BT_STACK_ALLOC_PATH((btsort_pyobject *)tree)

    Py_INCREF(item);
    rc = find_path_to_item(
            (btsort_pyobject *)tree, item, 1, 1, &path, &found);
    Py_DECREF(item);

    if (rc) return rc;

    if (!found) {
        PyErr_SetString(PyExc_ValueError, "btree removal: item not in btree");
        return -1;
    }

    Py_DECREF(path.lineage[path.depth]->values[path.indexes[path.depth]]);

    if (path.depth < path.tree->depth)
        branch_removal(&path);
    else
        leaf_removal(&path);

    return 0;
}


/*
 * python __iter__ implementation
 */
static PyTypeObject sorted_btree_iterator_type;

static PyObject *
python_sorted_btree_iter(PyObject *self) {
    int i;
    btsort_pyobject *tree = (btsort_pyobject *)self;
    btsort_iter_pyobject *iter = (btsort_iter_pyobject *)PyObject_GC_New(
            btsort_iter_pyobject, &sorted_btree_iterator_type);
    bt_node_t *node = tree->root;
    bt_path_t *path = malloc(sizeof(bt_path_t));

    path->depth = tree->depth;
    path->indexes = malloc(sizeof(int *) * tree->depth + 1);
    path->lineage = malloc(sizeof(bt_node_t *) * tree->depth + 1);
    path->tree = tree;
    iter->path = path;

    for (i = 0; i < tree->depth; ++i) {
        path->indexes[i] = 0;
        path->lineage[i] = node;
        node = ((bt_branch_t *)node)->children[0];
    }
    path->indexes[tree->depth] = 0;
    path->lineage[tree->depth] = node;

    Py_INCREF(self);

    return (PyObject *)iter;
}


/*
 * python btsort_iter_pyobject deallocator
 */
static void
sorted_btree_iterator_dealloc(btsort_iter_pyobject *self) {
    if (self->path == NULL) return;

    free(self->path->indexes);
    free(self->path->lineage);
    free(self->path);

    Py_DECREF(self->path->tree);

    PyObject_GC_UnTrack(self);
    self->ob_type->tp_free((PyObject *)self);
}


/*
 * python btsort_iter_pyobject.next implementation (the real iteration)
 */
static PyObject *
python_sorted_btreeiterator_next(btsort_iter_pyobject *iter) {
    PyObject *item;

    if (next_item(iter->path, &item)) {
        PyErr_SetString(PyExc_StopIteration, "");
        return NULL;
    }

    Py_INCREF(item);
    return item;
}


/*
 * python 'in' operator support
 */
static int
python_sorted_btree_contains(PyObject *self, PyObject *item) {
    btsort_pyobject *tree = (btsort_pyobject *)self;
    char found;
    BT_STACK_ALLOC_PATH(tree)

    if (find_path_to_item(tree, item, 1, 1, &path, (char *)(&found)))
        return -1;

    return (int)found;
}


/*
 * python bool() support
 */
static int
python_sorted_btree_nonzero(PyObject *self) {
    return ((btsort_pyobject *)self)->root->filled ? 1 : 0;
}


/*
 * python split() method
 */
static char *split_kwargs[] = {"separator", "eq_goes_left", NULL};

static PyObject *
python_sorted_btree_split(PyObject *self, PyObject *args, PyObject *kwargs) {
    btsort_pyobject *tree = (btsort_pyobject *)self;
    btsort_pyobject *new_tree;
    PyObject *item;
    PyObject *eq_goes_left = Py_True;
    PyObject *result;

    if (!PyArg_ParseTupleAndKeywords(
                args, kwargs, "O|O", split_kwargs, &item, &eq_goes_left))
        return NULL;

    BT_STACK_ALLOC_PATH(tree)

    /* trace out a path along which to cut */
    if (find_path_to_item(
            tree, item, !PyObject_IsTrue(eq_goes_left), 0, &path, NULL))
        return NULL;

    new_tree = PyObject_GC_New(btsort_pyobject, &btsort_pytypeobj);
    PyObject_GC_Track(new_tree);
    cut_tree(tree, path, new_tree);

    result = PyTuple_New(2);
    PyTuple_SET_ITEM(result, 0, (PyObject *)tree);
    PyTuple_SET_ITEM(result, 1, (PyObject *)new_tree);

    Py_INCREF(tree);

    return result;
}


/*
 * python bulkload class method
 */
static PyObject *
python_sorted_btree_bulkload(PyObject *klass, PyObject *args) {
    int i, result;
    PyObject *item_list, *order, *iter, *item, *prev = NULL;

    if (!PyArg_ParseTuple(args, "OO!", &item_list, &PyInt_Type, &order))
        return NULL;

    if ((iter = PyObject_GetIter(item_list)) == NULL)
        return NULL;
    for (i = 0, item = PyIter_Next(iter); item;
            ++i, item = PyIter_Next(iter)) {
        if (i) {
            result = PyObject_RichCompareBool(prev, item, Py_LT);
            if (result <= 0) {
                if (!result)
                    PyErr_SetString(PyExc_ValueError,
                            "the bulkloaded list must already be sorted");
                Py_DECREF(prev);
                Py_DECREF(item);
                Py_DECREF(iter);
                return NULL;
            }
            Py_DECREF(prev);
        }
        prev = item;
    }
    if (prev) Py_DECREF(prev);
    Py_DECREF(iter);

    if ((iter = PyObject_GetIter(item_list)) == NULL)
        return NULL;

    btsort_pyobject *tree = PyObject_GC_New(
            btsort_pyobject, &btsort_pytypeobj);

    if (bulkload(tree, iter, (int)PyInt_AsLong(order)))
        return NULL;

    return (PyObject *)tree;
}


/*
 * python copy method
 */
static PyObject *
python_sorted_btree_copy(PyObject *self, PyObject *args) {
    return (PyObject *)copy_tree((btsort_pyobject *)self);
}


/*
 * python 'after' method
 */
static PyObject *
python_sorted_btree_after(PyObject *self, PyObject *item) {
    PyObject *result;
    btsort_pyobject *tree = (btsort_pyobject *)self;
    bt_node_t *node;
    int index;
    BT_STACK_ALLOC_PATH(tree);

    if (find_path_to_leaf(tree, item, 0, &path))
        return NULL;

    node = path.lineage[path.depth];
    index = path.indexes[path.depth];

    while (index >= node->filled) {
        if (--path.depth < 0) {
            PyErr_SetString(PyExc_ValueError,
                    "item comes at the end of the sorted_btree");
            return NULL;
        }

        node = path.lineage[path.depth];
        index = path.indexes[path.depth];
    }

    result = node->values[index];
    Py_INCREF(result);
    return result;
}


/*
 * python 'before' method
 */

static PyObject *
python_sorted_btree_before(PyObject *self, PyObject *item) {
    PyObject *result;
    btsort_pyobject *tree = (btsort_pyobject *)self;
    bt_node_t *node;
    int index;
    BT_STACK_ALLOC_PATH(tree);

    if (find_path_to_leaf(tree, item, 1, &path))
        return NULL;

    node = path.lineage[path.depth];
    index = path.indexes[path.depth];

    while (index == 0) {
        if (--path.depth < 0) {
            PyErr_SetString(PyExc_ValueError,
                    "item comes at the end of the sorted_btree");
            return NULL;
        }

        node = path.lineage[path.depth];
        index = path.indexes[path.depth];
    }

    result = node->values[index - 1];
    Py_INCREF(result);
    return result;
}


static PyMethodDef sorted_btree_methods[] = {
    {"insert", python_sorted_btree_insert, METH_VARARGS,
        "\
insert an object into the sorted_btree\n\
\n\
:param obj: the object to insert\n\
\n\
:returns: ``None``\n\
"},
    {"remove", python_sorted_btree_remove, METH_VARARGS,
        "\
remove an object from the sorted_btree if found by == comparison\n\
\n\
:param obj: the object to remove\n\
\n\
:raises: ``ValueError`` if the object is not in the sorted_btree\n\
\n\
:returns: ``None``\n\
"},
    {"split", (PyCFunction)python_sorted_btree_split,
        METH_VARARGS | METH_KEYWORDS,
        "\
divide the sorted_btree into 2 by a separator value\n\
\n\
:param separator:\n\
    the split point -- objects greater than ``separator`` go into the left\n\
    sorted_btree, objects less than go into the right sorted_btree\n\
:param eq_goes_left:\n\
    if ``False`` then objects that are ``== separator`` go into the right\n\
    subtree, otherwise they go to the left. defaults to ``True``\n\
:type eq_goes_left: bool\n\
\n\
:returns:\n\
    a two-tuple of (``left_tree``, ``right_tree``). the ``left_tree`` is\n\
    actually the original tree, which has now been modified.\n\
"},
    {"copy", python_sorted_btree_copy, METH_NOARGS,
        "\
make a shallow copy of the btree\n\
\n\
:returns: a new btree with the same order, contents and structure\n\
"},
    {"__copy__", python_sorted_btree_copy, METH_NOARGS,
        "\
make a shallow copy of the btree\n\
\n\
:returns: a new btree with the same order, contents and structure\n\
"},
    {"after", python_sorted_btree_after, METH_O,
        "\
returns the item from the sorted_btree which would come next in sorted order\n\
\n\
:param item:\n\
    the object to compare against items contained in the sorted_btree\n\
\n\
:raises:\n\
    ``ValueError`` if the ``item`` argument would come after everything\n\
    contained in the sorted_btree\n\
\n\
:returns:\n\
    an object from the btree, the one that would appear next after the\n\
    ``item`` argument in sorted order\n\
"},
    {"before", python_sorted_btree_before, METH_O,
        "\
returns the item from the sorted_btree before the argument in sorted order\n\
\n\
:param item:\n\
    the object to compare against items contained in the sorted_btree\n\
\n\
:raises:\n\
    ``ValueError`` if the ``item`` argument would come before everything\n\
    contained in the sorted_btree\n\
\n\
:returns:\n\
    the object from the btree which would appear before the ``item``\n\
    argument in sorted order\n\
"},
    {"bulkload", python_sorted_btree_bulkload, METH_VARARGS | METH_CLASS,
        "\
create a sorted_btree from a pre-sorted list (classmethod)\n\
\n\
:param data: the items to load into the sorted_btree\n\
:type data: iterable\n\
:param order: the branching order for the sorted_btree\n\
:type order: int\n\
\n\
:returns: a new loaded sorted_btree instance\n\
"},
    {NULL, NULL, 0, NULL}
};

static PySequenceMethods sorted_btree_sequence_methods = {
    0,                            /* sq_length */
    0,                            /* sq_concat */
    0,                            /* sq_repeat */
    0,                            /* sq_item */
    0,                            /* sq_slice */
    0,                            /* sq_ass_item */
    0,                            /* sq_ass_slice */
    python_sorted_btree_contains, /* sq_contains */
    0,                            /* sq_inplace_concat */
    0                             /* sq_inplace_repeat */
};

static PyNumberMethods sorted_btree_number_methods = {
    0,                           /*nb_add*/
    0,                           /*nb_subtract*/
    0,                           /*nb_multiply*/
    0,                           /*nb_divide*/
    0,                           /*nb_remainder*/
    0,                           /*nb_divmod*/
    0,                           /*nb_power*/
    0,                           /*nb_negative*/
    0,                           /*nb_positive*/
    0,                           /*nb_absolute*/
    python_sorted_btree_nonzero, /*nb_nonzero*/
    0,                           /*nb_invert*/
    0,                           /*nb_lshift*/
    0,                           /*nb_rshift*/
    0,                           /*nb_and*/
    0,                           /*nb_xor*/
    0,                           /*nb_or*/
    0,                           /*nb_coerce*/
    0,                           /*nb_int*/
    0,                           /*nb_long*/
    0,                           /*nb_float*/
    0,                           /*nb_oct*/
    0,                           /*nb_hex*/
    0,                           /*nb_inplace_add*/
    0,                           /*nb_inplace_subtract*/
    0,                           /*nb_inplace_multiply*/
    0,                           /*nb_inplace_divide*/
    0,                           /*nb_inplace_remainder*/
    0,                           /*nb_inplace_power*/
    0,                           /*nb_inplace_lshift*/
    0,                           /*nb_inplace_rshift*/
    0,                           /*nb_inplace_and*/
    0,                           /*nb_inplace_xor*/
    0,                           /*nb_inplace_or*/
};

static PyMemberDef sorted_btree_members[] = {
    {"order", T_INT, sizeof(PyObject), READONLY,
        "The tree's order as passed to the constructor."},
    {"depth", T_INT, sizeof(PyObject) + sizeof(int), READONLY,
        "The current depth of the tree (just a single leaf is a depth of 0)"}
};

PyDoc_STRVAR(sorted_btree_class_doc, "\
A n-ary tree container type for sorted data\n\
\n\
The constructor takes 1 argument, the tree's order. This is an\n\
integer that indicates the most data items a single node may hold.");

/*
 * the full type definition for the python object
 */
PyTypeObject btsort_pytypeobj = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,
    "btree.sorted_btree",
    sizeof(btsort_pyobject),
    0,
    (destructor)sorted_btree_dealloc,          /* tp_dealloc */
    0,                                         /* tp_print */
    0,                                         /* tp_getattr */
    0,                                         /* tp_setattr */
    0,                                         /* tp_compare */
    (reprfunc)python_sorted_btree_repr,        /* tp_repr */
    &sorted_btree_number_methods,              /* tp_as_number */
    &sorted_btree_sequence_methods,            /* tp_as_sequence */
    0,                                         /* tp_as_mapping */
    0,                                         /* tp_hash */
    0,                                         /* tp_call */
    0,                                         /* tp_str */
    PyObject_GenericGetAttr,                   /* tp_getattro */
    0,                                         /* tp_setattro */
    0,                                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | /* tp_flags */
        Py_TPFLAGS_HAVE_GC,
    sorted_btree_class_doc,                    /* tp_doc */
    (traverseproc)sorted_btree_traverse,       /* tp_traverse */
    (inquiry)sorted_btree_clear,               /* tp_clear */
    0,                                         /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    python_sorted_btree_iter,                  /* tp_iter */
    0,                                         /* tp_iternext */
    sorted_btree_methods,                      /* tp_methods */
    sorted_btree_members,                      /* tp_members */
    0,                                         /* tp_getset */
    0,                                         /* tp_base */
    0,                                         /* tp_dict */
    0,                                         /* tp_descr_get */
    0,                                         /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    sorted_btree_object_init,                  /* tp_init */
    PyType_GenericAlloc,                       /* tp_alloc */
    PyType_GenericNew,                         /* tp_new */
    PyObject_GC_Del,                           /* tp_free */
};


/*
 * the sorted_btree iterator python object
 */
static PyTypeObject sorted_btree_iterator_type = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,
    "btree.sorted_btree_iterator",
    sizeof(btsort_iter_pyobject),
    0,
    (destructor)sorted_btree_iterator_dealloc,      /* tp_dealloc */
    0,                                              /* tp_print */
    0,                                              /* tp_getattr */
    0,                                              /* tp_setattr */
    0,                                              /* tp_compare */
    0,                                              /* tp_repr */
    0,                                              /* tp_as_number */
    0,                                              /* tp_as_sequence */
    0,                                              /* tp_as_mapping */
    0,                                              /* tp_hash */
    0,                                              /* tp_call */
    0,                                              /* tp_str */
    PyObject_GenericGetAttr,                        /* tp_getattro */
    0,                                              /* tp_setattro */
    0,                                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |      /* tp_flags */
        Py_TPFLAGS_HAVE_GC,
    0,                                              /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    (iternextfunc)python_sorted_btreeiterator_next, /* tp_iternext */
    0,                                              /* tp_methods */
    0,                                              /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    0,                                              /* tp_init */
    PyType_GenericAlloc,                            /* tp_alloc */
    PyType_GenericNew,                              /* tp_new */
    PyObject_GC_Del,                                /* tp_free */
};
