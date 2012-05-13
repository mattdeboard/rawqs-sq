import operator
import re
from collections import namedtuple
from haystack.query import SQ
from django.conf import settings

OPERATORS = {
    "AND": operator.and_,
    "OR": operator.or_,
    "NOT": operator.not_
}
HAYSTACK_DOCUMENT_FIELD = "text"
HAYSTACK_DEFAULT_OPERATOR = getattr(settings, 'HAYSTACK_DEFAULT_OPERATOR',
                                    'AND')

def parse(qs):
    content_terms = []
    Pair = namedtuple("Pair", "field term")
    # To do: Add re.U
    clause_re = re.compile("(\w+):")
    clauses = re.split(clause_re, qs)
    
    # If 'qs' starts with a field-less search term, it will be the 0th element
    # in the returned list. If 'qs' starts with 'field:term' pairs, it will be
    # an empty string. Either way, pop off the 0th element of the collection.
    content = clauses.pop(0)
    pairs = []
    if clauses:
        # Create a list of Pair instances based on zipping up field/term pairs
        # from the querystring.
        pairs = [Pair(i[0], i[1]) for i in zip(clauses[::2], clauses[1::2])]
        if content:
            pairs.append(Pair(HAYSTACK_DOCUMENT_FIELD, content[0]))

    top_sq = _field_pairs(pairs)
    
    if top_sq:
        return reduce(OPERATORS[HAYSTACK_DEFAULT_OPERATOR], top_sq)
    else:
        return None

def _field_pairs(pairs):
    for pair in pairs:
        # For now I'm only supporting very minimal 'field:term'
        # format querystrings just for simplicity's sake (also this is my use
        # case).
        term_comps = re.split(" (AND|OR|NOT) ", pair.term.strip("() "))
        subterms = term_comps[::2]
        sep = set(term_comps[1::2])

        if sep:
            # For right now I'm not supporting supreme complexity of these
            # querystrings. And at least for Solr having multiple boolean
            # operators in a single query without wrapping them in parens will
            # throw an error. So for now if there are different operators in the
            # same pair term, throw an exception.
            # To do: Write a custom exception for this.
            if len(sep) > 1:
                raise
            oper = OPERATORS[sep.pop()]
        else:
            oper = HAYSTACK_DEFAULT_OPERATOR

        yield reduce(oper, _subterms(subterms, field=pair.field))

def _subterms(subterms, field="content"):
    for subterm in subterms:
        if subterm.startswith('"') and subterm.endswith('"'):
            field = "%s__exact" % field
        else:
            field = field

        yield SQ([field, subterm])

