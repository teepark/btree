#ifndef PYBTREE_H
#define PYBTREE_H

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

#define PYBTREE_FLAG_INITED 1


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

#define PYBTREE_STACK_ALLOC_PATH(tree)   \
    path_t path;                         \
    int _indexes[(tree)->depth + 1];     \
    node_t *_lineage[(tree)->depth + 1]; \
    path.tree = (tree);                  \
    path.indexes = _indexes;             \
    path.lineage = _lineage;


/*
 * the PyObject of a btree iterator
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
int py_sorted_btree_insert(sorted_btree_object *, PyObject *);
int py_sorted_btree_remove(sorted_btree_object *, PyObject *);

#ifdef __cplusplus
}
#endif

#endif
