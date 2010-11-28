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
    int length;
    node_t *root;
    leaf_t *beginning;
} btreeobject;


/*
 * the necessary info to save our position and be
 * able to pick up traversing the tree in order
 */
typedef struct {
    int depth;
    int *indexes;
    node_t **lineage;

    btreeobject *tree;
} path_t;

#define PYBTREE_STACK_ALLOC_PATH(tree) \
    path_t path;                       \
    int _indexes[(tree)->depth];       \
    node_t *_lineage[(tree)->depth];   \
    path.tree = (tree);                \
    path.indexes = _indexes;           \
    path.lineage = _lineage;


/*
 * visitor function signatures for handing off to traversers
 */
typedef int (*nodevisitor)(
        node_t *node, char is_branch, int depth, void *data);

typedef int (*itemvisitor)(
        PyObject *item, char is_branch, int depth, void *data);


/*
 * function for btree insert
 */
int pybtree_insert(btreeobject *, PyObject *);
