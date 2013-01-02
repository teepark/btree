import errno
import os
from setuptools import Extension

from paver.easy import *
from paver.path import path
from paver.setuputils import setup


setup(
    name="btree",
    description="BTree implementation as a C python extension",
    version="0.2.0",
    author="Travis J Parker",
    author_email="travis.parker@gmail.com",
    url="http://github.com/teepark/btree",
    license="BSD",
    ext_modules=[Extension(
        'btree',
        ['src/btreemodule.c', 'src/sorted_btree.c'],
        include_dirs=())],
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: C",
    ]
)

MANIFEST = (
    "setup.py",
    "paver-minilib.zip",
    "src/btree_common.c",
    "src/btree_common.h",
    "src/btreemodule.c",
    "src/offsetstring.h",
    "src/sorted_btree.c",
    "src/sorted_btree.h",
)

@task
def manifest():
    path('MANIFEST.in').write_lines('include %s' % x for x in MANIFEST)

@task
@needs('generate_setup', 'minilib', 'manifest', 'setuptools.command.sdist')
def sdist():
    pass

@task
def clean():
    for p in map(path, ('btree.egg-info', 'dist', 'build', 'MANIFEST.in')):
        if p.exists():
            if p.isdir():
                p.rmtree()
            else:
                p.remove()
    for p in path(__file__).abspath().parent.walkfiles():
        if p.endswith(".pyc") or p.endswith(".pyo"):
            try:
                p.remove()
            except OSError, exc:
                if exc.args[0] == errno.EACCES:
                    continue
                raise

@task
def docs():
    sh("find docs -name *.rst | xargs touch; cd docs; make html")
