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

#include "btree_common.h"


/*
 * necessary forward declarations
 *
 * implementations must be provided by the individual btrees
 */
static bt_node_t* allocate_node(char is_branch, int order);

static void free_node(char is_branch, bt_node_t *node);

static void node_sizechange(bt_path_t *path);

static void node_pass_left(bt_path_t *path, int count);

static void node_pass_right(bt_path_t *path, int count);


/*
 * pass value[s] (and for branch nodes, child[ren]) to neighbors
 */
void
btnode_pass_left(bt_path_t *path, int count) {
    if (!count) return;
    char is_branch = path->depth < path->tree->depth;
    bt_branch_t *parent = (bt_branch_t *)(path->lineage[path->depth - 1]);
    int sep_index = path->indexes[path->depth - 1] - 1;
    bt_node_t *source = path->lineage[path->depth];
    bt_node_t *target = parent->children[sep_index];

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
        bt_branch_t *src = (bt_branch_t *)source;
        bt_branch_t *tgt = (bt_branch_t *)target;

        /* move over the same number of child nodes */
        memcpy(tgt->children + tgt->filled + 1, src->children,
                sizeof(bt_node_t *) * count);

        /* move over the source's remaining children */
        memmove(src->children, src->children + count,
                sizeof(bt_node_t *) * (src->filled - count + 1));
    }

    target->filled += count;
    source->filled -= count;
}

void
btnode_pass_right(bt_path_t *path, int count) {
    if (!count) return;
    char is_branch = path->depth < path->tree->depth;
    bt_branch_t *parent = (bt_branch_t *)(path->lineage[path->depth - 1]);
    int sep_index = path->indexes[path->depth - 1];
    bt_node_t *source = path->lineage[path->depth];
    bt_node_t *target = parent->children[sep_index + 1];

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
        bt_branch_t *src = (bt_branch_t *)source;
        bt_branch_t *tgt = (bt_branch_t *)target;

        /* make space for the same number of child nodes */
        memmove(tgt->children + count, tgt->children,
                sizeof(bt_node_t *) * (tgt->filled + 1));

        /* move over the children from source to target */
        memcpy(tgt->children, src->children + src->filled + 1 - count,
                sizeof(bt_node_t *) * count);
    }

    target->filled += count;
    source->filled -= count;
}


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
                node_pass_left(path, 1);
                return;
            }
        }

        /* try passing it right */
        if (parent_index < parent->filled) {
            sibling = parent->children[parent_index + 1];

            if (sibling->filled < path->tree->order) {
                node_pass_right(path, 1);
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
            path->lineage[path->depth] = right;
            path->indexes[path->depth - 1]++;
            node_pass_left(path, count);
            return;
        }
    }

    /* plan B: borrow from the neighbor to the left */
    if (parent_index) {
        left = parent->children[parent_index - 1];
        if (left->filled >= (path->tree->order / 2) + count) {
            /* there is a left neighbor, and it has enough spare items */
            path->lineage[path->depth] = left;
            path->indexes[path->depth - 1]--;
            node_pass_right(path, count);
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
heal_right_edge(bt_pyobject *tree) {
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
heal_left_edge(bt_pyobject *tree) {
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
 * splitting a btree along a path
 */
static void
cut_tree(bt_pyobject *tree, bt_path_t path, bt_pyobject *newtree) {
    bt_node_t *newroot, *node, *newnode, *parent;
    int depth, index;

    /* allocate a new root and tree */
    newroot = newnode = allocate_node(tree->depth, tree->order);

    newtree->order = tree->order;
    newtree->root = newroot;
    newtree->depth = tree->depth;
    newtree->flags = BT_FLAG_INITED;

    /* stack allocate a path for the new tree by hand */
    bt_path_t newpath;
    int _newpath_indexes[tree->depth + 1];
    bt_node_t *_newpath_lineage[tree->depth + 1];
    newpath.indexes = _newpath_indexes;
    newpath.lineage = _newpath_lineage;
    newpath.tree = newtree;

    /* follow the path down */
    for (depth = 0; depth <= tree->depth; ++depth) {
        node = path.lineage[depth];
        index = path.indexes[depth];

        newpath.indexes[depth] = 0;
        newpath.lineage[depth] = newnode;

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

        /* node sizes have changed */
        path.depth = newpath.depth = depth;
        node_sizechange(&path);
        node_sizechange(&newpath);

        /* create the node at the next depth
           and make it a child of the last one */
        parent = newnode;
        if (depth < tree->depth) {
            newnode = allocate_node(depth + 1 < tree->depth, tree->order);
            ((bt_branch_t *)parent)->children[0] = newnode;
        }
    }

    heal_right_edge(tree);
    heal_left_edge(newtree);
}


/*
 * loading a sorted list into a new btree
 */
static int
load_generation(PyObject **items, int item_count, bt_node_t **children,
        int order, char is_branch, bt_node_t **nodes, PyObject **separators) {
    int i, j, node_counter = 0, needed, offset;
    bt_node_t *node;
    bt_node_t *neighbor;

    /* this path object is for use in node_sizechange calls. the depth is
       always 0 so that every node put in it looks like a root, so that
       node_sizechange never iterates up to any parents */
    bt_path_t dummy_path;
    bt_node_t *_dummy_lineage[1];
    int _dummy_indexes[1];
    dummy_path.tree = NULL;
    dummy_path.depth = 0;
    dummy_path.indexes = _dummy_indexes;
    dummy_path.indexes[0] = 0;
    dummy_path.lineage = _dummy_lineage;
    dummy_path.lineage[0] = NULL;

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
        for (i = offset = 0; i <= node_counter; ++i) {
            node = nodes[i];
            for (j = 0; j <= node->filled; ++j)
                ((bt_branch_t *)node)->children[j] = children[offset++];

            /* record the size changes of the new nodes */
            dummy_path.lineage[0] = node;
            node_sizechange(&dummy_path);
        }

    return node_counter + 1;
}

static int
bulkload(bt_pyobject *tree, PyObject *iterable_source, int order) {
    PyObject_GC_Track(tree);
    int i, j, count = 0, sepsize = 64, depth = 0;
    PyObject *prev;
    PyObject **seps_next, **separators = malloc(sizeof(PyObject *) * sepsize);

    if ((prev = separators[0] = PyIter_Next(iterable_source))) {
        for (i = 1;; ++i) {
            if (i >= sepsize) {
                sepsize *= 2;

                seps_next = realloc(separators, sizeof(PyObject *) * sepsize);
                if (seps_next == NULL) {
                    for (j = 0; j < i; ++j) Py_DECREF(separators[j]);
                    free(separators);
                    PyObject_GC_UnTrack(tree);
                    PyObject_GC_Del(tree);
                    PyErr_SetString(PyExc_MemoryError, "failed to realloc");
                    return 1;
                }

                separators = seps_next;
            }

            separators[i] = PyIter_Next(iterable_source);
            if (separators[i] == NULL) break;

            prev = separators[i];
        }

        count = i;
    }

    bt_node_t *genX[(count / order) + 1], *genY[(count / order) + 1];

    Py_DECREF(iterable_source);

    /*
     * `genX` and `genY` alternate as the previous and current generation.
     *
     * using the `separators` array in this way (as both a to-read argument and
     * a to-write argument) looks dangerous, but the reading of `items` and the
     * writing of `separators` are both done left-to-right and the reading
     * starts first and outpaces the writing.
     *
     * we lose references to generations older than the previous one, but
     * children pointers are also set up in load_generation, so they are still
     * reachable.
     */
    count = (Py_ssize_t)load_generation(
            separators, count, NULL, order, 0, &(genX[0]), separators);
    while (count > 1) {
        count = (Py_ssize_t)load_generation(separators, count - 1, &(genX[0]),
                order, 1, &(genY[0]), separators);
        depth++;

        if (count <= 1) break;

        count = (Py_ssize_t)load_generation(separators, count - 1, &(genY[0]),
                order, 1, &(genX[0]), separators);
        depth++;
    }

    free(separators);

    tree->root = ((depth & 1) ? genY : genX)[0];
    tree->flags = BT_FLAG_INITED;
    tree->order = order;
    tree->depth = depth;

    return 0;
}


/*
 * a generalized node traverser
 */
static int
traverse_nodes_recursion(bt_node_t *node, int depth, int leaf_depth, char down,
        bt_node_visitor pred, void *data) {
    int i, rc;

    if (down && (rc = pred(node, depth != leaf_depth, depth, data)))
        return rc;

    if (depth < leaf_depth) {
        for (i = 0; i <= node->filled; ++i)
            if ((rc = traverse_nodes_recursion(
                    ((bt_branch_t *)node)->children[i],
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
traverse_nodes(
        bt_pyobject *tree, char down, bt_node_visitor pred, void *data) {
    return traverse_nodes_recursion(
            tree->root, 0, tree->depth, down, pred, data);
}


#if 0 /* until we have a use for this */
/*
 * generalized in-order python object traverser
 */
static int
traverse_items_recursion(bt_node_t *node, int depth, int leaf_depth,
        bt_item_visitor pred, void *data) {
    int i, rc;

    for (i = 0; i < node->filled; ++i) {
        if (depth != leaf_depth &&
                (rc = traverse_items_recursion(
                    ((bt_branch_t *)node)->children[i],
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
                ((bt_branch_t *)node)->children[i],
                depth + 1,
                leaf_depth,
                pred,
                data)))
        return rc;

    return 0;
}

static int
traverse_items(bt_pyobject *tree, bt_item_visitor pred, void *data) {
    return traverse_items_recursion(tree->root, 0, tree->depth, pred, data);
}
#endif


/*
 * reentrant generalized in-order item traversal
 */
static int
next_item(bt_path_t *path, PyObject **target) {
    int depth = path->depth;
    int index = path->indexes[depth];
    bt_node_t *node = path->lineage[depth];

    if (!path->tree->root->filled) return 1;

    if (depth < path->tree->depth) {
        /*
         * in a branch
         */

        /* the end condition */
        if (index >= node->filled)
            return 1;

        /* yield the value out of the branch */
        *target = node->values[index++];

        /* traverse down to the next leaf for the next call */
        path->indexes[depth]++;
        while (depth++ < path->tree->depth) {
            path->depth++;
            node = ((bt_branch_t *)node)->children[index];
            path->lineage[depth] = node;
            path->indexes[depth] = index = 0;
        }

        return 0;
    }

    /*
     * in a leaf
     */

    /* single leaf exit condition */
    if (path->tree->depth == 0 && index >= node->filled)
        return 1;

    /* yield the current value */
    *target = node->values[index++];
    path->indexes[depth]++;

    if (index < node->filled)
        return 0;

    /* traverse up to the next unfinished branch */
    while (depth--) {
        node = path->lineage[depth];
        index = path->indexes[depth];

        if (index < node->filled) {
            path->depth = depth;
            return 0;
        }
    }

    /* no unfinished branches, so set the error condition */
    path->depth = 0;
    return 0;
}
