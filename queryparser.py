import ast
import operator
import re
from collections import deque, namedtuple
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


class ClauseVisitor(ast.NodeVisitor):
    """
    Visit each node in the ad hoc syntax tree of a Solr querystring, and
    log certain kinds of visits in the 'nodestack' attribute. This log
    will then be used as instructions to ``build_sq`` to create SQ
    instances from a querystring.

    For more information about the NodeVisitor class, please see the docs
    at URL http://docs.python.org/library/ast.html#ast.NodeVisitor

    """
    def __init__(self):
        self.nodestack = deque()
        super(ClauseVisitor, self).__init__()
        
    def generic_visit(self, node):
        for field, value in reversed(list(ast.iter_fields(node))):
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, ast.AST):
                        self.visit(item)
            elif isinstance(value, ast.AST):
                self.visit(value)
            
    def visit_BoolOp(self, node):
        self.generic_visit(node)
        self.nodestack.append(type(node.op).__name__)
        
    def visit_Name(self, node):
        self.generic_visit(node)
        self.nodestack.append(node.id)

    def visit_Str(self, node):
        self.generic_visit(node)

        # We want to be able to handle Solr's range syntax, e.g.
        # "price:[100 TO 999]". To do this we'll have to wrap those bracketed
        # values in double quotes when the user is creating the querystring with
        # range syntax, e.g. 'price:"[100 TO 999]"', then remove them here.
        # By removing them we're removing the signal to `build_qs` to use the
        # __exact version of the query.
        if node.s.startswith('[') and node.s.endswith(']'):
            node_val = node.s
        else:
            node_val = '"%s"' % node.s
            
        self.nodestack.append(node_val)

        
def parse(qs, micromanage=False):
    """
    Parse a user-defined raw querystring 'qs' and return a single SQ
    object that expresses the same search. If 'micromanage'
    is set to True, this function will not perform processing of the term
    south. Instead, it will return an SQ where the term is the entirety of
    'pair.term', e.g. when micromanage == True:
    
    <SQ: OR (state__contains=Kentucky OR state__contains=Virginia OR
             state__exact=North Carolina)>

    when False:
    
    <SQ: AND state__contains=(Kentucky OR Virginia OR "North Carolina")>

    """
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
        pairs.append(Pair(HAYSTACK_DOCUMENT_FIELD, content))

    top_sq = field_pairs(pairs, micromanage=micromanage)
    
    if top_sq:
        return reduce(OPERATORS[HAYSTACK_DEFAULT_OPERATOR], top_sq)
    else:
        return None

def field_pairs(pairs, micromanage):
    """
    Yields an SQ object encapsulating the logic for the search terms
    specified for a single field. For example, for the querystring

    ``state:(California OR Oregon) title:("Best Buy" OR Target)``

    would yield two separate SQ objects from this function.

    Input is a list of ``Pair`` namedtuple instances. 
    
    """
    for pair in pairs:
        if micromanage:
            yield build_sq(pair.term, field=pair.field)
        else:
            yield SQ([pair.field, pair.term])

def build_sq(qs, field=HAYSTACK_DOCUMENT_FIELD, oper=operator.or_):
    """
    Return a single SQ object from an arbitrarily complex querystring for
    a single search field. This function uses the ``ast`` module's
    NodeVisitor class, subclassed above, to transform a querystring into
    a set of instructions. Those instructions are then used to compile
    'qs' into a single SQ object.

    Input is a parentheses-wrapped search term, e.g. ``(California OR
    Oregon)``.    

    """
        
    visitor = ClauseVisitor()
    nodelist = []
    opers = {
        'Or': operator.or_,
        'And': operator.and_
    }
    tree = ast.parse(qs.replace("OR", "or").replace("AND", "and"))
    # As a side effect of this function call, 'visitor' builds up its
    # 'nodestack' attribute which is a list of instructions to compile the
    # querystring into a single SQ object.
    visitor.visit(tree)
    
    for node in visitor.nodestack:
        if node in opers:
            # If 'node' is a recognized boolean operator, use its corresponding
            # function to reduce the last two SQ instances in 'nodelist' to a
            # single SQ instance. Then, ``pop()`` the last item off the list
            # then replace the (newly appointed) last item in the list with our
            # combined SQ instance.
            result = reduce(opers[node], nodelist[-2:])
            nodelist.pop()
            nodelist[-1] = result
        else:
            # If the user has wrapped their search term in quotes in order to
            # get an exact match, e.g. ``state:(Kentucky OR "North Carolina")``,
            # we'll use Haystack's ``__exact`` syntax to make sure that is
            # honored.
            if node.startswith('"') and node.endswith('"'):
                node = node.strip('"')
                f = "%s__exact" % field
            else:
                f = field
            nodelist.append(SQ([f, node]))

    # I am having an issue with this process in that in some cases, "OR" nodes
    # would be dropped from the field list produced by ``ast.iter_fields``. I've
    # compensated for it, I believe, but I am not sure it's reliable. It has
    # worked in my testing, but it feels very wobbly. Definitely needs another
    # evaluation.
    if len(nodelist) > 1:
        return reduce(oper, nodelist)
    else:
        return nodelist[0]
    
