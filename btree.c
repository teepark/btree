#include "Python.h"
#include "btree.h"

#define BOTHER_WITH_GC 1


/*
 * allocate space for a node
 */
static node_t*
allocate_node(char is_branch, int order) {
    node_t *node;
    if (is_branch) {
        node = malloc(sizeof(branch_t *));
        ((branch_t *)node)->children = malloc(sizeof(node_t *) * (order + 2));
    } else
        node = malloc(sizeof(leaf_t *));
    node->values = malloc(sizeof(PyObject *) * (order + 1));
    node->filled = 0;
    return node;
}


/*
 * deallocate a node
 */
static void
free_node(char is_branch, node_t *node) {
    int i;
    for (i = 0; i < node->filled; ++i)
        Py_DECREF(node->values[i]);
    free(node->values);
    if (is_branch)
        /* relax, this only frees the child pointers,
         * not the child nodes themselves. */
        free(((branch_t *)node)->children);
    free(node);
}


/*
 * pass value[s] (and for branch nodes, child[ren]) to the left neighbor
 */
static void
pass_left(char is_branch, node_t *source, node_t *target, int count,
        branch_t *parent, int sep_index) {
    if (!count) return;

    /* append the parent's separator value to the target */
    target->values[target->filled] = parent->values[sep_index];

    /* use the last value to be moved from the source as the new separator */
    parent->values[sep_index] = source->values[count - 1];

    /* continue appending from the beginning of the source's values */
    memcpy(target->values + target->filled + 1, source->values,
            sizeof(PyObject *) * (count - 1));

    /* move down the source's remaining values to the beginning */
    memmove(source->values, source->values + count,
            sizeof(PyObject *) * (source->filled - count));

    if (is_branch) {
        branch_t *src_b = (branch_t *)source;
        branch_t *tgt_b = (branch_t *)target;

        /* move over the same number of child nodes */
        memcpy(tgt_b->children + tgt_b->filled + 1, src_b->children,
                sizeof(node_t *) * count);

        /* move over the source's remaining children */
        memmove(src_b->children, src_b->children + count,
                sizeof(node_t *) * (src_b->filled - count + 1));
    }

    target->filled += count;
    source->filled -= count;
}


/*
 * pass value[s] (and for branch nodes, child[ren]) to the right neighbor
 */
static void
pass_right(char is_branch, node_t *source, node_t *target, int count,
        branch_t *parent, int sep_index) {
    if (!count) return;

    /* make space in the target for the moved item(s) */
    memmove(target->values + count, target->values,
            sizeof(PyObject *) * target->filled);

    /* prepend the parent's separator value to the target */
    target->values[count - 1] = parent->values[sep_index];

    /* copy the first item to be moved into the parent */
    parent->values[sep_index] = source->values[source->filled - 1];

    /* copy the other items to be moved into the target */
    memcpy(target->values, source->values - count,
            sizeof(PyObject *) * (count - 1));

    if (is_branch) {
        branch_t *src = (branch_t *)source;
        branch_t *tgt = (branch_t *)target;

        /* make space for the same number of child nodes */
        memmove(tgt->children + count, tgt->children,
                sizeof(node_t *) * (tgt->filled + 1));

        /* move over the children from source to target */
        memcpy(tgt->children, src->children + src->filled,
                sizeof(node_t *) * count);
    }

    target->filled += count;
    source->filled -= count;
}


/*
 * shrink a node to preserve the btree invariants
 */
static void
shrink_node(path_t *path) {
    int parent_index = -1;
    branch_t *parent = NULL;
    node_t *sibling;
    node_t *node = path->lineage[path->depth];

    int middle;
    PyObject *median;

    /* if we aren't the root, try passing an item to a neighbor */
    if (path->depth) {
        parent_index = path->indexes[path->depth - 1];
        parent = (branch_t *)path->lineage[path->depth - 1];

        /* try passing it left first */
        if (parent_index) {
            sibling = parent->children[parent_index - 1];
            if (sibling->filled < path->tree->order) {
                pass_left(path->depth < path->tree->depth, node, sibling, 1,
                        parent, parent_index - 1);
                return;
            }
        }

        /* try passing it right */
        if (parent_index < parent->filled) {
            sibling = parent->children[parent_index + 1];

            if (sibling->filled < path->tree->order) {
                pass_right(path->depth < path->tree->depth, node, sibling, 1,
                        parent, parent_index);
                return;
            }
        }
    }

    /*
     * fallback plan: split the current node into two
     */

    /* pull the median value, it's going up to the parent node */
    middle = node->filled / 2;
    median = node->values[middle];

    /* put the second half of the node's data in a new sibling */
    sibling = allocate_node(
            path->depth < path->tree->depth, path->tree->order);
    sibling->filled = node->filled - middle - 1;
    memcpy(sibling->values, node->values + middle + 1,
            sizeof(PyObject *) * sibling->filled);
    if (path->depth < path->tree->depth)
        memcpy(((branch_t *)sibling)->children,
                ((branch_t *)node)->children + middle + 1,
                sizeof(node_t *) * (node->filled - middle));

    /* cut off the original node's data in the middle */
    node->filled = middle;

    /*
     * push the median value up to the parent
     */

    if (!(path->depth)) {
        /* if we were the root, we're going to need a parent now */
        parent = (branch_t *)allocate_node(1, path->tree->order);

        /* insert the siblings as children, and the median value */
        parent->children[0] = node;
        parent->children[1] = sibling;
        parent->values[0] = median;
        parent->filled = 1;
        path->tree->root = (node_t *)parent;
        path->tree->depth++;
    } else {
        /* if we need to, make space in the parent's arrays */
        if (parent_index < parent->filled) {
            memmove(parent->values + parent_index + 1,
                    parent->values + parent_index,
                    sizeof(PyObject *) * (parent->filled - parent_index));
            memmove(parent->children + parent_index + 2,
                    parent->children + parent_index + 1,
                    sizeof(node_t *) * (parent->filled - parent_index));
        }

        parent->values[parent_index] = median;
        parent->children[parent_index + 1] = sibling;
        parent->filled += 1;
    }

    /* now if the parent node is overflowed then it too needs to shrink */
    if (parent->filled > path->tree->order) {
        path->depth--;
        shrink_node(path);
    }
}


/*
 * find the leftmost index in a PyObject array into which a
 * value is insertable still preserving sorted order
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


/*
 * given a path all the way to a leaf, insert a value into it
 */
static void
leaf_insert(PyObject *value, path_t *path) {
    leaf_t *leaf = path->lineage[path->depth];
    int index = path->indexes[path->depth];

    /* put the value in place */
    if (index < leaf->filled)
        memmove(leaf->values + index + 1, leaf->values + index,
                (leaf->filled - index) * sizeof(PyObject *));
    memcpy(leaf->values + index, &value, sizeof(PyObject *));

    leaf->filled++;

    /* now if the node is overfilled, correct it */
    if (leaf->filled > path->tree->order)
        shrink_node(path);
}


/*
 * trace a path to the appropriate leaf for insertion
 */
static int
find_path_to_leaf(btreeobject *tree, PyObject *value, path_t *path) {
    int i, index;
    node_t *node = (node_t *)(tree->root);

    path->tree = tree;

    for (i = 0; i <= tree->depth; ++i) {
        if (i) node = ((branch_t *)node)->children[index];

        if ((index = bisect_left(node->values, node->filled, value)) < 0)
            return index;

        path->lineage[i] = node;
        path->indexes[i] = index;
    }
    path->depth = tree->depth;
    return 0;
}


/*
 * a generalized node traverser
 */
static int
traverse_nodes_recursion(node_t *node, int depth, int leaf_depth, char down,
        nodevisitor pred, void *data) {
    int i, rc;

    if (down && (rc = pred(node, depth != leaf_depth, depth, data)))
        return rc;

    if (depth != leaf_depth) {
        for (i = 0; i <= node->filled; ++i)
            if ((rc = traverse_nodes_recursion(
                    ((branch_t *)node)->children[i],
                    depth + 1,
                    leaf_depth,
                    down,
                    pred,
                    data)))
                return rc;
    }

    if (!down && (rc = pred(node, depth != leaf_depth, depth, data)))
        return rc;

    return 0;
}

static int
traverse_nodes(btreeobject *tree, char down, nodevisitor pred, void *data) {
    return traverse_nodes_recursion(tree->root, 0, tree->depth, down, pred, data);
}


/*
 * generalized in-order python object traverser
 */
static int
traverse_items_recursion(node_t *node, int depth, int leaf_depth,
        itemvisitor pred, void *data) {
    int i, rc;

    for (i = 0; i < node->filled; ++i) {
        if (depth != leaf_depth &&
                (rc = traverse_items_recursion(
                    ((branch_t *)node)->children[i],
                    depth + 1,
                    leaf_depth,
                    pred,
                    data)))
            return rc;

        if ((rc = pred(node->values[i], depth != leaf_depth, depth, data)))
            return rc;
    }

    if (depth != leaf_depth &&
            (rc = traverse_items_recursion(
                ((branch_t *)node)->children[i],
                depth + 1,
                leaf_depth,
                pred,
                data)))
        return rc;

    return 0;
}

static int
traverse_items(btreeobject *tree, itemvisitor pred, void *data) {
    return traverse_items_recursion(tree->root, 0, tree->depth, pred, data);
}


/*
 * initializer for the python type
 */
static char *btree_init_args[] = {"order", NULL};

static int
btreeobject_init(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *order;
    btreeobject *tree = (btreeobject *)self;

    if (!PyArg_ParseTupleAndKeywords(
                args, kwargs, "O!", btree_init_args, &PyInt_Type, &order))
        return -1;

    tree->order = (int)PyInt_AsLong(order);
    tree->depth = 0;
    tree->root = allocate_node(0, tree->order);

    return 0;
}


/*
 * python btree instance deallocator
 */
static int
dealloc_visitor(node_t *node, char is_branch, int depth, void *data) {
    free_node(is_branch, node);
    return 0;
}

static void
btree_dealloc(btreeobject *self) {
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

static int traverse_visitor(node_t *node, char is_branch, int depth,
        void *payload) {
    int i;
    visitproc visit = ((traverse_payload *)(payload))->visit;
    void *arg = ((traverse_payload *)(payload))->arg;
    for (i = 0; i < node->filled; ++i) {
        Py_VISIT(node->values[i]);
    }
    return 0;
}

static int
btree_traverse(btreeobject *self, visitproc visit, void *arg) {
    traverse_payload payload = {visit, arg};
    return traverse_nodes(self, 1, traverse_visitor, (void *)(&payload));
}


/*
 * python btree GC clearer
 */
static int
clear_visitor(node_t *node, char is_branch, int depth, void *payload) {
    int i;
    for (i = 0; i < node->filled; ++i)
        Py_CLEAR(node->values[i]);
    return 0;
}

static int
btree_clear(btreeobject *self) {
    return traverse_nodes(self, 0, clear_visitor, NULL);
}


/*
 * repr handling
 */
#define INITIAL_SIZE 0x1000

typedef struct offsetstring {
    char *data;
    int offset; /* amount full */
    int length; /* total capacity */
} offsetstring;

static int
ensure_space(offsetstring *string, long length) {
    if (string->length - string->offset < length) {
        while (string->length - string->offset < length) string->length *= 2;
        string->data = (char *)realloc(string->data, string->length);
        if (string->data == NULL) {
            PyErr_SetString(PyExc_MemoryError, "failed to reallocate");
            return ENOMEM;
        }
    }
    return 0;
}

static int
repr_visit(node_t *node, char is_branch, int depth, void *data) {
    offsetstring *string = (offsetstring *)data;
    int i, j, rc;
    PyObject *reprd;
    char item_str[INITIAL_SIZE];

    if ((rc = ensure_space(string, depth * 2)))
        return rc;

    for (i = 0; i < depth; ++i) {
        string->data[string->offset] = string->data[string->offset + 1] = ' ';
        string->offset += 2;
    }

    j = sprintf(item_str, "<%s filled=%d values=(",
            is_branch ? "BRANCH" : "LEAF",
            node->filled);

    for (i = 0; i < node->filled; ++i) {
        reprd = PyObject_Repr(node->values[i]);
        if (!reprd) return -1;
        j += sprintf(item_str + j, "%s, ", PyString_AsString(reprd));
        Py_DECREF(reprd);
    }

    if (node->filled) j -= 2;

    if ((rc = ensure_space(string, j + 3)))
        return rc;

    memcpy(string->data + string->offset, item_str, j);
    memcpy(string->data + string->offset + j, ")>\n", 3);
    string->offset += j + 3;

    if ((rc = ensure_space(string, 1)))
        return rc;

    string->data[string->offset] = '\0';
    return 0;
}

static PyObject *
python_btree_repr(PyObject *self) {
    btreeobject *tree = (btreeobject *)self;
    PyObject *result;
    offsetstring string = {malloc(INITIAL_SIZE), 0, INITIAL_SIZE};

    if (traverse_nodes(tree, 1, repr_visit, (void *)(&string)))
        result = NULL;
    else
        result = PyString_FromStringAndSize(string.data, string.offset - 1);

    free(string.data);
    return result;
}


/*
 * python insert method for btree objects
 */
static PyObject *
python_btree_insert(PyObject *self, PyObject *args) {
    PyObject *item;
    btreeobject *tree = (btreeobject *)self;
    PYBTREE_STACK_ALLOC_PATH(tree);

    if (!PyArg_ParseTuple(args, "O", &item)) return NULL;

    Py_INCREF(item);

    if (find_path_to_leaf(tree, item, &path)) {
        Py_DECREF(item);
        return NULL;
    }

    leaf_insert(item, &path);

    Py_INCREF(Py_None);
    return Py_None;
}


/*
 * C api function for btree insert
 */
int
pybtree_insert(btreeobject *tree, PyObject *item) {
    int rc;
    PYBTREE_STACK_ALLOC_PATH(tree);

    Py_INCREF(item);
    if ((rc = find_path_to_leaf(tree, item, &path))) {
        Py_DECREF(item);
        return rc;
    }

    leaf_insert(item, &path);
    return 0;
}


/*
 * python as_list method for getting the items out
 */
static int
as_list_item_visitor(PyObject *item, char is_branch, int depth, void *data) {
    return PyList_Append((PyObject *)data, item);
}

static PyObject *
python_btree_as_list(PyObject *self, PyObject *args) {
    btreeobject *tree = (btreeobject *)self;
    int rc;
    PyObject *list = PyList_New(0);

    if ((rc = traverse_items(tree, as_list_item_visitor, (void *)list)))
        return NULL;

    return list;
}


static PyMethodDef btree_methods[] = {
    {"insert", python_btree_insert, METH_VARARGS, "no docs yet"},
    {"as_list", python_btree_as_list, METH_NOARGS, "no docs yet"},
    {NULL, NULL, 0, NULL}
};

/*
 * the full type definition for the python object
 */
static PyTypeObject btreetype = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,
    "btree.btree",
    sizeof(btreeobject),
    0,
    (destructor)btree_dealloc,                 /* tp_dealloc */
    0,                                         /* tp_print */
    0,                                         /* tp_getattr */
    0,                                         /* tp_setattr */
    0,                                         /* tp_compare */
    (reprfunc)python_btree_repr,               /* tp_repr */
    0,                                         /* tp_as_number */
    0,                                         /* tp_as_sequence */
    0,                                         /* tp_as_mapping */
    0,                                         /* tp_hash */
    0,                                         /* tp_call */
    0,                                         /* tp_str */
    PyObject_GenericGetAttr,                   /* tp_getattro */
    0,                                         /* tp_setattro */
    0,                                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | /* tp_flags */
#if BOTHER_WITH_GC
        Py_TPFLAGS_HAVE_GC,
    0,                                         /* tp_doc */
    (traverseproc)btree_traverse,              /* tp_traverse */
    (inquiry)btree_clear,                      /* tp_clear */
#else
        0,
    0,                                         /* tp_doc */
    0,                                         /* tp_traverse */
    0,                                         /* tp_clear */
#endif
    0,                                         /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter */
    0,                                         /* tp_iternext */
    btree_methods,                             /* tp_methods */
    0,                                         /* tp_members */
    0,                                         /* tp_getset */
    0,                                         /* tp_base */
    0,                                         /* tp_dict */
    0,                                         /* tp_descr_get */
    0,                                         /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    btreeobject_init,                          /* tp_init */
    PyType_GenericAlloc,                       /* tp_alloc */
    PyType_GenericNew,                         /* tp_new */
#if BOTHER_WITH_GC
    PyObject_GC_Del,                           /* tp_free */
#else
    PyObject_Del,                              /* tp_free */
#endif
};


/*
 * module initializer
 */
PyMODINIT_FUNC
initbtree(void) {
    PyObject *module = Py_InitModule("btree", NULL);
    Py_INCREF(&btreetype);

    if (PyType_Ready(&btreetype) < 0)
        return;
    PyModule_AddObject(module, "btree", (PyObject *)(&btreetype));
}
