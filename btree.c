#include "Python.h"
#include "structmember.h"

#include "btree.h"

#define PRINTF(format_str, py_object) {                                \
    PyObject *__printf_pyobj = PyObject_Repr((PyObject *)(py_object)); \
    printf((format_str), PyString_AsString(__printf_pyobj));           \
    Py_DECREF(__printf_pyobj);                                         \
}


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
    parent->values[sep_index] = source->values[source->filled - count];

    /* copy the other items to be moved into the target */
    memcpy(target->values, source->values + source->filled - count + 1,
            sizeof(PyObject *) * (count - 1));

    if (is_branch) {
        branch_t *src = (branch_t *)source;
        branch_t *tgt = (branch_t *)target;

        /* make space for the same number of child nodes */
        memmove(tgt->children + count, tgt->children,
                sizeof(node_t *) * (tgt->filled + 1));

        /* move over the children from source to target */
        memcpy(tgt->children, src->children + src->filled + 1 - count,
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
 * grow a node to preserve the btree invariants
 */
static void
grow_node(path_t *path, int count) {
    branch_t *parent = (branch_t *)(path->lineage[path->depth - 1]);
    int parent_index = path->indexes[path->depth - 1];
    node_t *right, *left, *node;
    right = left = NULL;
    node = path->lineage[path->depth];

    /* plan A: borrow from the neighbor to the right */
    if (parent_index + 1 <= parent->filled) {
        right = parent->children[parent_index + 1];
        if (right->filled >= (path->tree->order / 2) + count) {
            /* there is a right neighbor, and it has enough spare items */
            pass_left(path->depth < path->tree->depth, right, node, count,
                    parent, parent_index);
            return;
        }
    }

    /* plan B: borrow from the neighbor to the left */
    if (parent_index) {
        left = parent->children[parent_index - 1];
        if (left->filled >= (path->tree->order / 2) + count) {
            /* there is a left neighbor, and it has enough spare items */
            pass_right(path->depth < path->tree->depth, left, node, count,
                    parent, parent_index - 1);
            return;
        }
    }

    /* TODO: plan B-and-a-half: see if left and right neighbors have
             enough items to spare combined, and pull some from each */

    /* plan C: merge with the left neighbor */
    if (left != NULL) {
        /* presuming that `count` is the minimum number required to make `node`
           a legal node, then any existing sibling that didn't have enough
           extras to spare is small enough to legally combine with `node` */

        /* append the separator from the parent to left */
        left->values[left->filled] = parent->values[parent_index - 1];

        /* copy over the items from node to left */
        memcpy(left->values + left->filled + 1, node->values,
                sizeof(PyObject *) * node->filled);

        /* if applicable, copy the children over as well */
        if (path->depth < path->tree->depth) {
            memcpy(((branch_t *)left)->children + left->filled + 1,
                    ((branch_t *)node)->children,
                    sizeof(node_t *) * (node->filled + 1));
        }

        if (parent->filled > parent_index) {
            /* remove the separator from the parent */
            if (parent->filled > parent_index)
            memmove(parent->values + parent_index - 1,
                    parent->values + parent_index,
                    sizeof(PyObject *) * (parent->filled - parent_index));

            /* also remove the child pointer to node */
            memmove(parent->children + parent_index,
                    parent->children + parent_index + 1,
                    sizeof(node_t *) * (parent->filled - parent_index));
        }

        parent->filled--;
        left->filled += node->filled + 1;

        /* finally, deallocate the dropped node */
        free_node(path->depth < path->tree->depth, node);
    } else {
        /* plan D: merge with the right neighbor */

        /* append the separator from the parent to node */
        node->values[node->filled] = parent->values[parent_index];

        /* copy the items from right to node */
        memcpy(node->values + node->filled + 1, right->values,
                sizeof(PyObject *) * (right->filled));

        /* if applicable, copy the children as well */
        if (path->depth < path->tree->depth) {
            memcpy(((branch_t *)node)->children + node->filled + 1,
                    ((branch_t *)right)->children,
                    sizeof(node_t *) * (right->filled + 1));
        }

        if (parent->filled > parent_index) {
            /* remove the separator from the parent */
            memmove(parent->values + parent_index,
                    parent->values + parent_index + 1,
                    sizeof(PyObject *) * (parent->filled - parent_index - 1));

            /* and remove the child pointer to right */
            memmove(parent->children + parent_index + 1,
                    parent->children + parent_index + 2,
                    sizeof(node_t *) * (parent->filled - parent_index - 1));
        }

        parent->filled--;
        node->filled += right->filled + 1;

        /* finally, deallocate the dropped node */
        free_node(path->depth < path->tree->depth, right);
    }

    if (path->depth > 1 && parent->filled < (path->tree->order / 2)) {
        path->depth--;
        grow_node(path, 1);
    } else if (path->depth == 1 && !(parent->filled)) {
        free_node(1, path->tree->root);
        path->tree->root = (left == NULL ? node : left);
        path->tree->depth--;
    }
}


/*
 * remove an item from a leaf
 */
static void
leaf_removal(path_t *path) {
    node_t *node = path->lineage[path->depth];
    int index = path->indexes[path->depth];

    /* move all the items after it down a space, covering it up */
    if (index < node->filled - 1)
        memmove(node->values + index, node->values + index + 1,
                sizeof(PyObject *) * (node->filled - index - 1));
    node->filled--;

    /* if the node is now too small, borrow from or merge with a sibling */
    if (path->depth && node->filled < (path->tree->order / 2)) {
        grow_node(path, 1);
    }
}


/*
 * remove an item from a branch
 */
static void
branch_removal(path_t *path) {
    branch_t *node = (branch_t *)(path->lineage[path->depth]);
    node_t *descendent;
    int index = path->indexes[path->depth];
    int i;

    /* try promoting from the right subtree first,
       but only if it won't result in a rebalance */
    descendent = node->children[index + 1];
    for (i = path->depth + 1; i < path->tree->depth; ++i) {
        path->lineage[i] = descendent;
        path->indexes[i] = 0;
        descendent = ((branch_t *)descendent)->children[0];
    }
    path->lineage[path->tree->depth] = descendent;
    path->indexes[path->tree->depth] = 0;

    if (descendent->filled > (path->tree->order / 2)) {
        node->values[index] = descendent->values[0];

        path->depth = path->tree->depth;
        leaf_removal(path);
        return;
    }

    /* TODO: still borrow from the right if it will be able to borrow
     *       from _it's_ right sibling */

    /* fallback to promoting from the left subtree */
    descendent = node->children[index];
    for (i = path->depth + 1; i < path->tree->depth; ++i) {
        path->lineage[i] = descendent;
        path->indexes[i] = descendent->filled;
        descendent = ((branch_t *)descendent)->children[descendent->filled];
    }
    path->lineage[path->tree->depth] = descendent;
    path->indexes[path->tree->depth] = descendent->filled - 1;

    node->values[index] = descendent->values[descendent->filled - 1];
    path->depth = path->tree->depth;
    leaf_removal(path);
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
 * trace a path to an item matching a given python value
 */
static int
find_path_to_item(
        btreeobject *tree, PyObject *value, path_t *path, char *found) {
    int i, index, cmp = 0;
    node_t *node = (node_t *)(tree->root);

    *found = 0;
    path->tree = tree;

    for (i = 0; i <= tree->depth; ++i) {
        if (i) node = ((branch_t *)node)->children[index];

        if ((index = bisect_left(node->values, node->filled, value)) < 0)
            return index;

        if (index < node->filled && (cmp = PyObject_RichCompareBool(
                node->values[index], value, Py_EQ)) < 0)
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
 * splitting a btree by a separator value
 */
static int
cut_node(node_t *node, PyObject *separator, int depth, int leaf_depth,
        int order, char eq_goes_left, node_t **new_node) {
    /*
     * this function recurses down the tree doing destructure operations at
     * every step *and* has failure cases, but it can still all be done safely.
     *
     * this is because it copies data to the newly created node on the way down
     * the tree, but doesn't do any destructive operation on the original node
     * until it comes back up, after the last possible failure case has gone by
     */
    int index, rc;
    int (*bisector)(PyObject **, int, PyObject *);

    bisector = eq_goes_left ? bisect_right : bisect_left;
    if ((index = bisector(node->values, node->filled, separator)) < 0)
        return index;

    *new_node = allocate_node(depth < leaf_depth, order);

    memcpy((*new_node)->values, node->values + index,
            sizeof(PyObject *) * (node->filled - index));

    if (depth < leaf_depth) {
        memcpy(((branch_t *)(*new_node))->children + 1,
                ((branch_t *)node)->children + index + 1,
                sizeof(node_t *) * (node->filled - index));

        if ((rc = cut_node(((branch_t *)node)->children[index],
                        separator,
                        depth + 1,
                        leaf_depth,
                        order,
                        eq_goes_left,
                        ((branch_t *)(*new_node))->children))) {
            free_node(depth < leaf_depth, *new_node);
            return rc;
        }
    }

    (*new_node)->filled = node->filled - index;
    node->filled = index;

    return 0;
}

static int
cut_tree(btreeobject *tree, PyObject *separator, char eq_goes_left,
        node_t **new_root) {
    return cut_node(tree->root, separator, 0, tree->depth, tree->order,
            eq_goes_left, new_root);
}


/*
 * repairing the damage along a freshly cut edge
 */
static void
heal_right_edge(btreeobject *tree) {
    int i, j, original_depth = tree->depth;
    node_t *node, *next;

    /* first pass, decrement the tree depth and free
     * the root for every consecutive empty root */
    node = tree->root;
    for (i = 0; i < original_depth; ++i) {
        next = ((branch_t *)node)->children[node->filled];
        if (node->filled)
            break;
        else {
            free_node(1, node);
            tree->depth--;
            tree->root = node = next;
        }
    }
    original_depth = tree->depth;

    /* second pass, grow any nodes that are too small along the right edge */
    PYBTREE_STACK_ALLOC_PATH(tree);
    for (i = 1; i <= tree->depth; ++i) {
        if (tree->depth < original_depth) {
            i--;
            original_depth = tree->depth;
        }

        node = tree->root;
        for (j = 0; j <= i; ++j) {
            path.lineage[j] = node;
            path.indexes[j] = node->filled;
            if (j < i) node = ((branch_t *)node)->children[node->filled];
        }
        path.depth = i;

        if (node->filled < (tree->order / 2))
            grow_node(&path, (tree->order / 2) - node->filled);
    }
}

static void
heal_left_edge(btreeobject *tree) {
    int i, j, original_depth = tree->depth;
    node_t *node, *next;

    /* first pass -- decrement the tree depth and free
     * the root for every consecutive empty root */
    node = tree->root;
    for (i = 0; i < original_depth; ++i) {
        next = ((branch_t *)node)->children[0];
        if (node->filled)
            break;
        else {
            free_node(1, node);
            tree->depth--;
            tree->root = node = next;
        }
    }

    /* second pass, grow any nodes that are too small along the right edge */
    PYBTREE_STACK_ALLOC_PATH(tree);
    for (i = 1; i <= tree->depth; ++i) {
        if (tree->depth < original_depth) {
            i--;
            original_depth = tree->depth;
        }

        node = tree->root;
        for (j = 0; j <= i; ++j) {
            path.lineage[j] = node;
            path.indexes[j] = 0;
            if (j < i) node = ((branch_t *)node)->children[0];
        }
        path.depth = i;

        if (node->filled < (tree->order / 2))
            grow_node(&path, (tree->order / 2) - node->filled);
    }
}


/*
 * loading a sorted list into a new btree
 */
static PyTypeObject btreetype;

static int
load_generation(PyObject **items, int item_count, node_t **children, int order,
        char is_branch, node_t **nodes, PyObject **separators) {
    int i, j, node_counter = 0, needed, children_offset;
    node_t *node;
    node_t *neighbor;

    /*
     * pull `order` items into a new node, then 1 item as a separator, repeat
     */
    node = allocate_node(is_branch, order);
    for (i = 0; i < item_count; ++i) {
        if (i % (order + 1) == order) {
            /* at a separator value */
            node->filled = order;
            nodes[node_counter] = node;
            separators[node_counter] = items[i];
            node = allocate_node(is_branch, order);
            node_counter++;
        }
        else {
            /* still inside a node */
            node->values[i % (order + 1)] = items[i];
        }
    }
    node->filled = i % (order + 1);
    nodes[node_counter] = node;

    /*
     * if the generation's final node didn't wind up with enough items,
     * pass enough over from the penultimate node to make both legal
     */
    if (node_counter && node->filled < (order / 2)) {
        /* get some items from the previous node to make everything legal */
        needed = (order / 2) - node->filled;
        neighbor = nodes[node_counter - 1];

        /* make space for the items to be moved */
        memmove(node->values + needed, node->values,
                sizeof(PyObject *) * node->filled);

        /* prepend the item from the separators */
        node->values[needed - 1] = separators[node_counter - 1];

        /* make the first item to be moved be the separator */
        separators[node_counter - 1] = neighbor->values[
            neighbor->filled - needed];

        /* copy the other items to be moved */
        memcpy(node->values, neighbor->values + neighbor->filled - needed + 1,
                sizeof(PyObject *) * (needed - 1));

        node->filled += needed;
        neighbor->filled -= needed;
    }

    /*
     * if a non-leaf generation, hand out the children to the new nodes
     */
    if (is_branch)
        for (i = children_offset = 0; i <= node_counter; ++i) {
            node = nodes[i];
            for (j = 0; j <= node->filled; ++j)
                ((branch_t *)node)->children[j] = children[children_offset++];
        }

    return node_counter + 1;
}

static btreeobject *
bulkload(PyObject *item_list, int order) {
    btreeobject *tree = PyObject_GC_New(btreeobject, &btreetype);
    PyObject_GC_Track(tree);
    int i, count, depth = 0;
    node_t *genX[(PyList_GET_SIZE(item_list) / order) + 1];
    node_t *genY[(PyList_GET_SIZE(item_list) / order) + 1];
    PyObject *separators[PyList_GET_SIZE(item_list)];

    count = PyList_GET_SIZE(item_list);
    for (i = 0; i < count;  ++i) {
        separators[i] = PyList_GET_ITEM(item_list, i);
        Py_INCREF(separators[i]);
    }

    /*
     * `genX` and `genY` alternate as the previous and current generation.
     *
     * using the `separators` array in this way (as both a to-read argument and
     * a to-write argument) looks dangerous, but the reading of `items` and the
     * writing of `separators` are both done left-to-right and the reading
     * outpaces the writing.
     *
     * we lose references to generations older than the previous one, but
     * children pointers are also set up in load_generation, so they are still
     * reachable.
     */
    count = load_generation(separators, count, NULL, order, 0, &(genX[0]),
            separators);
    while (count > 1) {
        count = load_generation(separators, count - 1, &(genX[0]), order, 1,
                &(genY[0]), separators);
        depth++;

        if (count <= 1) break;

        count = load_generation(separators, count - 1, &(genY[0]), order, 1,
                &(genX[0]), separators);
        depth++;
    }

    tree->root = ((depth & 1) ? genY : genX)[0];
    tree->flags = PYBTREE_FLAG_INITED;
    tree->order = order;
    tree->depth = depth;

    return tree;
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

    if (depth < leaf_depth) {
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


#if 0 /* until we have a use for this */
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
#endif


/*
 * reentrant generalized in-order item traversal
 */
static int
next_item(path_t *path, PyObject **ptr) {
    int depth = path->depth;
    int index = path->indexes[depth];
    node_t *node = path->lineage[depth];

    if (!path->tree->root->filled) return 1;

    if (depth < path->tree->depth) {
        /*
         * in a branch
         */

        /* the end condition */
        if (index >= node->filled)
            return 1;

        /* yield the value out of the branch */
        *ptr = node->values[index++];

        /* traverse down to the next leaf for the next call */
        path->indexes[depth]++;
        while (depth++ < path->tree->depth) {
            path->depth++;
            path->lineage[depth] = node = ((branch_t *)node)->children[index];
            path->indexes[depth] = index = 0;
        }

        return 0;
    }

    /*
     * in a leaf
     */

    /* yield the current value */
    *ptr = node->values[index++];

    if (index < node->filled) {
        path->indexes[depth]++;
        return 0;
    }

    /* traverse up to the next unfinished branch */
    while (depth--) {
        node = path->lineage[depth];
        index = path->indexes[depth];

        if (index <= node->filled - 1) {
            path->depth = depth;
            return 0;
        }
    }

    /* no unfinished branches, so set the error condition */
    path->depth = 0;
    path->indexes[0] = (path->lineage[0])->filled;
    return 0;
}


/*
 * initializer for the python type
 */
static char *btree_init_args[] = {"order", NULL};

static int
btreeobject_init(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *order;
    btreeobject *tree = (btreeobject *)self;

    tree->flags = 0;

    if (!PyArg_ParseTupleAndKeywords(
                args, kwargs, "O!", btree_init_args, &PyInt_Type, &order))
        return -1;

    tree->order = (int)PyInt_AsLong(order);
    tree->depth = 0;
    tree->root = allocate_node(0, tree->order);
    tree->flags |= PYBTREE_FLAG_INITED;

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
dealloc_visitor(node_t *node, char is_branch, int depth, void *data) {
    free_node(is_branch, node);
    return 0;
}

static void
btree_dealloc(btreeobject *self) {
    PyObject_GC_UnTrack(self);
    if (self->flags & PYBTREE_FLAG_INITED)
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
        if (reprd == NULL) return -1;
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

    return 0;
}

static PyObject *
python_btree_repr(PyObject *self) {
    btreeobject *tree = (btreeobject *)self;
    PyObject *result;
    int rc;
    if ((rc = Py_ReprEnter(self))) {
        if (rc < 0) return NULL;
        return PyString_FromString("<...>");
    }

    offsetstring string = {malloc(INITIAL_SIZE), 0, INITIAL_SIZE};

    if (traverse_nodes(tree, 1, repr_visit, (void *)(&string)))
        result = NULL;
    else
        result = PyString_FromStringAndSize(string.data, string.offset - 1);

    Py_ReprLeave(self);
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

    if (find_path_to_leaf(tree, item, &path)) {
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
pybtree_insert(btreeobject *tree, PyObject *item) {
    int rc;
    PYBTREE_STACK_ALLOC_PATH(tree);

    rc = find_path_to_leaf(tree, item, &path);
    if (rc) return rc;

    Py_INCREF(item);
    leaf_insert(item, &path);
    return 0;
}


/*
 * python remove() method for deletion
 */
static PyObject *
python_btree_remove(PyObject *self, PyObject *args) {
    btreeobject *tree = (btreeobject *)self;
    PyObject *item;
    char found;
    PYBTREE_STACK_ALLOC_PATH(tree);

    if (!PyArg_ParseTuple(args, "O", &item)) return NULL;

    if (find_path_to_item(tree, item, &path, &found))
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
pybtree_remove(btreeobject *tree, PyObject *item) {
    int rc;
    char found;
    PYBTREE_STACK_ALLOC_PATH(tree);

    Py_INCREF(item);
    rc = find_path_to_item(tree, item, &path, &found);
    Py_DECREF(item);

    if (rc) return -1;

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
static PyTypeObject btreeiteratortype;

static PyObject *
python_btree_iter(PyObject *self) {
    int i;
    btreeobject *tree = (btreeobject *)self;
    btreeiterator *iter = (btreeiterator *)PyObject_New(
            btreeiterator, &btreeiteratortype);
    node_t *node = tree->root;
    path_t *path = malloc(sizeof(path_t));

    path->depth = tree->depth;
    path->indexes = malloc(sizeof(int *) * tree->depth + 1);
    path->lineage = malloc(sizeof(node_t *) * tree->depth + 1);
    path->tree = tree;
    iter->path = path;

    for (i = 0; i < tree->depth; ++i) {
        path->indexes[i] = 0;
        path->lineage[i] = node;
        node = ((branch_t *)node)->children[0];
    }
    path->indexes[tree->depth] = 0;
    path->lineage[tree->depth] = node;

    Py_INCREF(self);

    return (PyObject *)iter;
}


/*
 * python btree_iterator deallocator
 */
static void
btree_iterator_dealloc(btreeiterator *self) {
    if (self->path == NULL) return;

    free(self->path->indexes);
    free(self->path->lineage);
    free(self->path);

    Py_DECREF(self->path->tree);
}


/*
 * python btree_iterator.next implementation (the real iteration)
 */
static PyObject *
python_btreeiterator_next(btreeiterator *iter) {
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
python_btree_contains(PyObject *self, PyObject *item) {
    btreeobject *tree = (btreeobject *)self;
    char found;
    PYBTREE_STACK_ALLOC_PATH(tree)

    if (find_path_to_item(tree, item, &path, (char *)(&found)))
        return -1;

    return (int)found;
}


/*
 * python split() method
 */
static char *split_kwargs[] = {"separator", "eq_goes_left", NULL};

static PyObject *
python_btree_split(PyObject *self, PyObject *args, PyObject *kwargs) {
    btreeobject *tree = (btreeobject *)self;
    btreeobject *new_tree;
    node_t *new_root;
    PyObject *item;
    PyObject *eq_goes_left = Py_True;
    PyObject *result;

    if (!PyArg_ParseTupleAndKeywords(
                args, kwargs, "O|O", split_kwargs, &item, &eq_goes_left))
        return NULL;

    if (cut_tree(tree, item, PyObject_IsTrue(eq_goes_left), &new_root))
        return NULL;

    new_tree = PyObject_GC_New(btreeobject, &btreetype);
    PyObject_GC_Track(new_tree);
    new_tree->root = new_root;
    new_tree->order = tree->order;
    new_tree->depth = tree->depth;
    new_tree->flags = PYBTREE_FLAG_INITED;

    heal_left_edge(new_tree);
    heal_right_edge(tree);

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
python_btree_bulkload(PyObject *klass, PyObject *args) {
    PyObject *item_list, *order;

    if (!PyArg_ParseTuple(args, "O!O!", &PyList_Type, &item_list,
            &PyInt_Type, &order))
        return NULL;

    return (PyObject *)bulkload(item_list, (int)PyInt_AsLong(order));
}


static PyMethodDef btree_methods[] = {
    {"insert", python_btree_insert, METH_VARARGS,
        "insert a comparable object into the btree"},
    {"remove", python_btree_remove, METH_VARARGS,
        "remove the object from the btree if found,\n\
         otherwise raises ValueError"},
    {"split", (PyCFunction)python_btree_split, METH_VARARGS | METH_KEYWORDS,
        "divide the tree into 2 by a separator value"},
    {"bulkload", python_btree_bulkload, METH_VARARGS | METH_CLASS,
        "create a btree from a pre-sorted list"},
    {NULL, NULL, 0, NULL}
};

static PySequenceMethods btree_sequence_methods = {
    0,                     /* sq_length */
    0,                     /* sq_concat */
    0,                     /* sq_repeat */
    0,                     /* sq_item */
    0,                     /* sq_slice */
    0,                     /* sq_ass_item */
    0,                     /* sq_ass_slice */
    python_btree_contains, /* sq_contains */
    0,                     /* sq_inplace_concat */
    0                      /* sq_inplace_repeat */
};

static PyMemberDef btree_members[] = {
    {"order", T_INT, sizeof(PyObject), READONLY,
        "The tree's order as passed to the constructor."},
    {"depth", T_INT, sizeof(PyObject) + sizeof(int), READONLY,
        "The current depth of the tree (just a single leaf is a depth of 0)"}
};

PyDoc_STRVAR(btree_class_doc, "A n-ary tree container type for sorted data\n\
    \n\
    The constructor takes 1 argument, the tree's order. This is an\n\
    integer that indicates the most data items a single node may hold.");

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
    &btree_sequence_methods,                   /* tp_as_sequence */
    0,                                         /* tp_as_mapping */
    0,                                         /* tp_hash */
    0,                                         /* tp_call */
    0,                                         /* tp_str */
    PyObject_GenericGetAttr,                   /* tp_getattro */
    0,                                         /* tp_setattro */
    0,                                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | /* tp_flags */
        Py_TPFLAGS_HAVE_GC,
    btree_class_doc,                           /* tp_doc */
    (traverseproc)btree_traverse,              /* tp_traverse */
    (inquiry)btree_clear,                      /* tp_clear */
    0,                                         /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    python_btree_iter,                         /* tp_iter */
    0,                                         /* tp_iternext */
    btree_methods,                             /* tp_methods */
    btree_members,                             /* tp_members */
    0,                                         /* tp_getset */
    0,                                         /* tp_base */
    0,                                         /* tp_dict */
    0,                                         /* tp_descr_get */
    0,                                         /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    btreeobject_init,                          /* tp_init */
    PyType_GenericAlloc,                       /* tp_alloc */
    PyType_GenericNew,                         /* tp_new */
    PyObject_GC_Del,                           /* tp_free */
};


/*
 * the btree iterator python object
 */
static PyTypeObject btreeiteratortype = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,
    "btree.btree_iterator",
    sizeof(btreeiterator),
    0,
    (destructor)btree_iterator_dealloc,        /* tp_dealloc */
    0,                                         /* tp_print */
    0,                                         /* tp_getattr */
    0,                                         /* tp_setattr */
    0,                                         /* tp_compare */
    0,                                         /* tp_repr */
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
        0,
    0,                                         /* tp_doc */
    0,                                         /* tp_traverse */
    0,                                         /* tp_clear */
    0,                                         /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    0,                                         /* tp_iter */
    (iternextfunc)python_btreeiterator_next,   /* tp_iternext */
    0,                                         /* tp_methods */
    0,                                         /* tp_members */
    0,                                         /* tp_getset */
    0,                                         /* tp_base */
    0,                                         /* tp_dict */
    0,                                         /* tp_descr_get */
    0,                                         /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    0,                                         /* tp_init */
    PyType_GenericAlloc,                       /* tp_alloc */
    PyType_GenericNew,                         /* tp_new */
    PyObject_GC_Del,                           /* tp_free */
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
