# Ensure we get the local copy of tornado instead of what's on the standard path
import os
import sys

sys.path.insert(0, os.path.abspath(".."))
import mingmq

master_doc = "index"

project = "MingMQ"
copyright = "The MingMQ Authors"

version = release = mingmq.version

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
]

primary_domain = "py"
default_role = "py:obj"

autodoc_member_order = "bysource"
autoclass_content = "both"
autodoc_inherit_docstrings = False

# Without this line sphinx includes a copy of object.__init__'s docstring
# on any class that doesn't define __init__.
# https://bitbucket.org/birkenfeld/sphinx/issue/1337/autoclass_content-both-uses-object__init__
autodoc_docstring_signature = False

coverage_skip_undoc_in_source = True
coverage_ignore_modules = [
]
# I wish this could go in a per-module file...
coverage_ignore_classes = [
]

coverage_ignore_functions = [
]

html_favicon = "favicon.ico"

intersphinx_mapping = {"python": ("https://docs.python.org/3.8/", None)}

on_rtd = os.environ.get("READTHEDOCS", None) == "True"

# On RTD we can't import sphinx_rtd_theme, but it will be applied by
# default anyway.  This block will use the same theme when building locally
# as on RTD.
if not on_rtd:
    import sphinx_rtd_theme

    html_theme = "sphinx_rtd_theme"
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]