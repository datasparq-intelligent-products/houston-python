
from string import Template


class PlanTemplate(Template):
    idpattern = "(?a:unsafe_substitution)"
    braceidpattern = "(?a:[_a-z][_a-z0-9]*)"
