#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import copy
import random
import string
import unittest

import btree


class SortedBTreeTestMixin(object):
    @property
    def data(self):
        return copy.deepcopy(self.DATA)

    def tree(self):
        return btree.sorted_btree(self.order)

    def test_iteration(self):
        tree = self.tree()
        map(tree.insert, self.data)

        for a, b in zip(tree, sorted(self.data)):
            self.assertEqual(a, b)

    def test_as_boolean(self):
        tree = self.tree()

        self.assertFalse(tree)

        tree.insert(self.data[0])
        self.assertTrue(tree)

        map(tree.insert, self.data[1:])
        self.assertTrue(tree)

    def test_contents_released(self):
        # booleans aren't subclassable so let's just skip this one for now
        if any(isinstance(x, bool) for x in self.data):
            return

        tree = self.tree()
        keys = set()
        key = 1

        def itemdel(self):
            keys.remove(self._key)

        data = self.data
        items = []
        for item in data:
            newtype = type('', (type(item),), {'__del__': itemdel})
            item = newtype(item)
            item._key = key
            key += 1
            keys.add(item._key)
            tree.insert(item)
            items.append(item)
        del item

        self.assertEqual(len(keys), len(self.data))

        map(tree.remove, items)
        del items[:]

        self.assertEqual(keys, set())


def generate(targets):
    for name, data in targets.iteritems():
        globals()[name + 'Test'] = type(name + 'Test',
                (SortedBTreeTestMixin, unittest.TestCase),
                {'DATA': data['data'], 'order': data.get('order', 64)})

def randstr(length):
    return ''.join(random.choice(string.ascii_letters) for i in xrange(length))


generate({
    'IntShortRange': {'data': range(-10, 10)},
    'IntLongRange': {'data': range(-1000, 1000)},

    'StrShort': {'data': [randstr(random.randrange(20)) for i in xrange(20)]},
    'StrLong': {'data': [randstr(random.randrange(20)) for i in xrange(2000)]},

    'FloatShortRange': {'data': map(float, range(-10, 10))},
    'FloatLongRange': {'data': map(float, range(-1000, 1000))},

    'LotsOfBools': {'data': [True, False] * 5000},
})


if __name__ == '__main__':
    unittest.main()
