import errno
import os
from setuptools import Extension

from paver.easy import *
from paver.path import path
from paver.setuputils import setup


setup(
    name="btree",
    description="BTree implementation as a C python extension",
    version="0.1",
    author="Travis J Parker",
    author_email="travis.parker@gmail.com",
    url="http://github.com/teepark/btree",
    license="BSD",
    ext_modules=[Extension(
        'btree',
        ['btree.c'],
        include_dirs=())],
    classifiers = [
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: C",
        #"Topic :: System :: Archiving :: Compression",
    ]
)

MANIFEST = (
    "setup.py",
    "paver-minilib.zip",
    "btree.c",
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
