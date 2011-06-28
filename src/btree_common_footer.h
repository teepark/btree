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

/*
 * the contents of this file would generally go in a .c file, but it is for
 * #include-ing after some forward declarations (or implementations) of
 * functions which *will* need to be specific to the kind of btree.
 *
 * the specific interface needed is:
 *  - bt_node_t* allocate_node(char is_branch, int order)
 *  - void free_node(char is_branch, bt_node_t *node)
 *  - void node_sizechange(bt_path_t *path)
 *  - void node_pass_left(char is_branch, bt_node_t *source,
 *                        bt_node_t *target, int count,
 *                        bt_branch_t *parent, int sep_index)
 *  - void node_pass_right(char is_branch, bt_node_t *source,
 *                         bt_node_t *target, int count,
 *                         bt_branch_t *parent, int sep_index)
 *  - static int find_path_to_item(btsort_pyobject *tree, PyObject *value,
 *                                 char first, char find, bt_path_t *path,
 *                                 char *found)
 */


/*
 * shrink a node by at least 1 to preserve the btree invariants
 */
static void
shrink_node(bt_path_t *path) {
    int parent_index = -1;
    bt_branch_t *parent = NULL;
    bt_node_t *sibling;
    bt_node_t *node = path->lineage[path->depth];

    int middle;
    PyObject *median;

    /* if we aren't the root, try passing an item to a neighbor */
    if (path->depth) {
        parent_index = path->indexes[path->depth - 1];
        parent = (bt_branch_t *)path->lineage[path->depth - 1];

        /* try passing it left first */
        if (parent_index) {
            sibling = parent->children[parent_index - 1];
            if (sibling->filled < path->tree->order) {
                node_pass_left(path->depth < path->tree->depth, node,
                        sibling, 1, parent, parent_index - 1);
                return;
            }
        }

        /* try passing it right */
        if (parent_index < parent->filled) {
            sibling = parent->children[parent_index + 1];

            if (sibling->filled < path->tree->order) {
                node_pass_right(path->depth < path->tree->depth, node,
                        sibling, 1, parent, parent_index);
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
        memcpy(((bt_branch_t *)sibling)->children,
                ((bt_branch_t *)node)->children + middle + 1,
                sizeof(bt_node_t *) * (node->filled - middle));

    /* cut off the original node's data in the middle */
    node->filled = middle;

    /* the node's size has changed */
    path->lineage[path->depth] = node;
    node_sizechange(path);

    /* the new sibling's size has changed */
    path->indexes[path->depth - 1]++;
    path->lineage[path->depth] = sibling;
    node_sizechange(path);

    /*
     * push the median value up to the parent
     */

    if (!(path->depth)) {
        /* if we were the root, we're going to need a parent now */
        parent = (bt_branch_t *)allocate_node(1, path->tree->order);

        /* insert the siblings as children, and the median value */
        parent->children[0] = node;
        parent->children[1] = sibling;
        parent->values[0] = median;
        parent->filled = 1;
        path->tree->root = (bt_node_t *)parent;
        path->tree->depth++;
    } else {
        /* if we need to, make space in the parent's arrays */
        if (parent_index < parent->filled) {
            memmove(parent->values + parent_index + 1,
                    parent->values + parent_index,
                    sizeof(PyObject *) * (parent->filled - parent_index));
            memmove(parent->children + parent_index + 2,
                    parent->children + parent_index + 1,
                    sizeof(bt_node_t *) * (parent->filled - parent_index));
        }

        parent->values[parent_index] = median;
        parent->children[parent_index + 1] = sibling;
        parent->filled += 1;
    }

    /* the parent's size has changed in either of the above branches */
    path->depth--;
    node_sizechange(path);

    /* now if the parent node is overflowed then it too needs to shrink */
    if (parent->filled > path->tree->order)
        shrink_node(path);
}


/*
 * grow a node to preserve the btree invariants
 */
static void
grow_node(bt_path_t *path, int count) {
    bt_branch_t *parent = (bt_branch_t *)(path->lineage[path->depth - 1]);
    int parent_index = path->indexes[path->depth - 1];
    bt_node_t *right, *left, *node;
    right = left = NULL;
    node = path->lineage[path->depth];

    /* plan A: borrow from the neighbor to the right */
    if (parent_index + 1 <= parent->filled) {
        right = parent->children[parent_index + 1];
        if (right->filled >= (path->tree->order / 2) + count) {
            /* there is a right neighbor, and it has enough spare items */
            node_pass_left(path->depth < path->tree->depth, right, node, count,
                    parent, parent_index);
            return;
        }
    }

    /* plan B: borrow from the neighbor to the left */
    if (parent_index) {
        left = parent->children[parent_index - 1];
        if (left->filled >= (path->tree->order / 2) + count) {
            /* there is a left neighbor, and it has enough spare items */
            node_pass_right(path->depth < path->tree->depth, left, node, count,
                    parent, parent_index - 1);
            return;
        }
    }

    /* TODO: plan C: see if left and right neighbors combined have
             enough items to spare, and if so pull some from each */

    /* plan D: merge with the left neighbor */
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
            memcpy(((bt_branch_t *)left)->children + left->filled + 1,
                    ((bt_branch_t *)node)->children,
                    sizeof(bt_node_t *) * (node->filled + 1));
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
                    sizeof(bt_node_t *) * (parent->filled - parent_index));
        }

        parent->filled--;
        left->filled += node->filled + 1;

        /* the left node's size has changed */
        path->indexes[path->depth - 1]--;
        path->lineage[path->depth] = left;
        node_sizechange(path);

        /* finally, deallocate the dropped node */
        free_node(path->depth < path->tree->depth, node);
    } else {
        /* plan E: merge with the right neighbor */

        /* append the separator from the parent to node */
        node->values[node->filled] = parent->values[parent_index];

        /* copy the items from right to node */
        memcpy(node->values + node->filled + 1, right->values,
                sizeof(PyObject *) * (right->filled));

        /* if applicable, copy the children as well */
        if (path->depth < path->tree->depth) {
            memcpy(((bt_branch_t *)node)->children + node->filled + 1,
                    ((bt_branch_t *)right)->children,
                    sizeof(bt_node_t *) * (right->filled + 1));
        }

        if (parent->filled > parent_index) {
            /* remove the separator from the parent */
            memmove(parent->values + parent_index,
                    parent->values + parent_index + 1,
                    sizeof(PyObject *) * (parent->filled - parent_index - 1));

            /* and remove the child pointer to right */
            memmove(parent->children + parent_index + 1,
                    parent->children + parent_index + 2,
                    sizeof(bt_node_t *) * (parent->filled - parent_index - 1));
        }

        parent->filled--;
        node->filled += right->filled + 1;

        /* the node's and its parent's sizes have changed */
        node_sizechange(path);

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
leaf_removal(bt_path_t *path) {
    bt_node_t *node = path->lineage[path->depth];
    int index = path->indexes[path->depth];

    /* move all the items after it down a space, covering it up */
    if (index < node->filled - 1)
        memmove(node->values + index, node->values + index + 1,
                sizeof(PyObject *) * (node->filled - index - 1));
    node->filled--;
    node_sizechange(path);

    /* if the node is now too small, borrow from or merge with a sibling */
    if (path->depth && node->filled < (path->tree->order / 2))
        grow_node(path, 1);
}


/*
 * remove an item from a branch
 */
static void
branch_removal(bt_path_t *path) {
    bt_branch_t *node = (bt_branch_t *)(path->lineage[path->depth]);
    bt_node_t *descendent;
    int index = path->indexes[path->depth];
    int i;

    /* try promoting from the right subtree first,
       but only if it won't result in a rebalance */
    descendent = node->children[index + 1];
    for (i = path->depth + 1; i < path->tree->depth; ++i) {
        path->lineage[i] = descendent;
        path->indexes[i] = 0;
        descendent = ((bt_branch_t *)descendent)->children[0];
    }
    path->lineage[path->tree->depth] = descendent;
    path->indexes[path->tree->depth] = 0;

    if (descendent->filled > (path->tree->order / 2)) {
        node->values[index] = descendent->values[0];

        path->depth = path->tree->depth;
        leaf_removal(path);
        return;
    }

    /* fallback to promoting from the left subtree */
    descendent = node->children[index];
    for (i = path->depth + 1; i < path->tree->depth; ++i) {
        path->lineage[i] = descendent;
        path->indexes[i] = descendent->filled;
        descendent = ((bt_branch_t *)descendent)->children[descendent->filled];
    }
    path->lineage[path->tree->depth] = descendent;
    path->indexes[path->tree->depth] = descendent->filled - 1;

    node->values[index] = descendent->values[descendent->filled - 1];
    path->depth = path->tree->depth;
    leaf_removal(path);
}


/*
 * given a path all the way to a leaf, insert a value into it
 */
static void
leaf_insert(PyObject *value, bt_path_t *path) {
    bt_leaf_t *leaf = path->lineage[path->depth];
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
    else
        node_sizechange(path);
}


/*
 * repairing the damage along a freshly cut edge
 */
static void
heal_right_edge(btsort_pyobject *tree) {
    int i, j, original_depth = tree->depth;
    bt_node_t *node, *next;

    /* first pass, decrement the tree depth and free
     * the root for every consecutive empty root */
    node = tree->root;
    for (i = 0; i < original_depth; ++i) {
        next = ((bt_branch_t *)node)->children[node->filled];
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
    BT_STACK_ALLOC_PATH(tree)
    for (i = 1; i <= tree->depth; ++i) {
        if (tree->depth < original_depth) {
            i--;
            original_depth = tree->depth;
        }

        node = tree->root;
        for (j = 0; j <= i; ++j) {
            path.lineage[j] = node;
            path.indexes[j] = node->filled;
            if (j < i) node = ((bt_branch_t *)node)->children[node->filled];
        }
        path.depth = i;

        if (node->filled < (tree->order / 2))
            grow_node(&path, (tree->order / 2) - node->filled);
    }
}

static void
heal_left_edge(btsort_pyobject *tree) {
    int i, j, original_depth = tree->depth;
    bt_node_t *node, *next;

    /* first pass -- decrement the tree depth and free
     * the root for every consecutive empty root */
    node = tree->root;
    for (i = 0; i < original_depth; ++i) {
        next = ((bt_branch_t *)node)->children[0];
        if (node->filled)
            break;
        else {
            free_node(1, node);
            tree->depth--;
            tree->root = node = next;
        }
    }

    /* second pass, grow any nodes that are too small along the right edge */
    BT_STACK_ALLOC_PATH(tree)
    for (i = 1; i <= tree->depth; ++i) {
        if (tree->depth < original_depth) {
            i--;
            original_depth = tree->depth;
        }

        node = tree->root;
        for (j = 0; j <= i; ++j) {
            path.lineage[j] = node;
            path.indexes[j] = 0;
            if (j < i) node = ((bt_branch_t *)node)->children[0];
        }
        path.depth = i;

        if (node->filled < (tree->order / 2))
            grow_node(&path, (tree->order / 2) - node->filled);
    }
}


/*
 * splitting a btree by a separator value
 */
static btsort_pyobject *
cut_tree(btsort_pyobject *tree, PyObject *separator, char eq_goes_left) {
    btsort_pyobject *newtree;
    bt_node_t *newroot, *node, *newnode, *parent;
    int depth, index;
    BT_STACK_ALLOC_PATH(tree)

    /* trace out a path to follow as we cut down the tree */
    if (find_path_to_item(tree, separator, !eq_goes_left, 0, &path, NULL))
        return NULL;

    /* allocate a new root */
    newroot = newnode = allocate_node(tree->depth, tree->order);

    /* follow the path down */
    for (depth = 0; depth <= tree->depth; ++depth) {
        node = path.lineage[depth];
        index = path.indexes[depth];

        /* copy values across to the new node */
        memcpy(newnode->values, node->values + index,
                sizeof(PyObject *) * (node->filled - index));

        /* copy children to the new node */
        if (depth < tree->depth)
            memcpy(((bt_branch_t *)newnode)->children + 1,
                    ((bt_branch_t *)node)->children + index + 1,
                    sizeof(bt_node_t *) * (node->filled - index));

        /* set the sizes of the nodes */
        newnode->filled = node->filled - index;
        node->filled = index;


        /* create the node at the next depth
           and make it a child of the last one */
        parent = newnode;
        if (depth < tree->depth) {
            newnode = allocate_node(depth + 1 < tree->depth, tree->order);
            ((bt_branch_t *)parent)->children[0] = newnode;
        }
    }

    newtree = PyObject_GC_New(btsort_pyobject, &btsort_pytypeobj);
    PyObject_GC_Track(newtree);
    newtree->root = newroot;
    newtree->order = tree->order;
    newtree->depth = tree->depth;
    newtree->flags = BT_FLAG_INITED;

    heal_right_edge(tree);
    heal_left_edge(newtree);

    return newtree;
}
