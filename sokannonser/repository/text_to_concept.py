import logging
from flashtext.keyword import KeywordProcessor
from elasticsearch import ElasticsearchException
from elasticsearch.helpers import scan

log = logging.getLogger(__name__)


class TextToConcept(object):
    COMPETENCE_KEY = 'KOMPETENS'
    OCCUPATION_KEY = 'YRKE'
    TRAIT_KEY = 'FORMAGA'
    REMOVED_TAG = '<removed>'

    def __init__(self, esclient, ontologyindex='narvalontology'):
        self.keyword_processor = KeywordProcessor()
        self.client = esclient
        self._init_keyword_processor(ontologyindex)

    def _init_keyword_processor(self, ontologyindex):
        [self.keyword_processor.add_non_word_boundary(token)
         for token in list('åäöÅÄÖ()')]

        concept_to_term = {}
        if self.client:
            for term_obj in self._elastic_iterator(ontologyindex):
                self.keyword_processor.add_keyword(term_obj['term'], term_obj)
                concept_preferred_label = term_obj['concept'].lower()
                if concept_preferred_label not in concept_to_term:
                    concept_to_term[concept_preferred_label] = []
                concept_to_term[concept_preferred_label].append(term_obj)

    def _elastic_iterator(self, index, size=1000):
        elastic_query = {
            "query": {
                "match_all": {}
            }
        }

        scan_result = scan(self.client, elastic_query, index=index, size=size)
        try:
            for row in scan_result:
                yield row['_source']
        except ElasticsearchException as e:
            log.error("Failed to load ontology (%s)" % str(e))

    def text_to_concepts(self, text):
        ontology_concepts = self._get_concepts(text, concept_type=None, span_info=False)
        text_lower = text.lower()

        tmp_text = text_lower

        print("OC", ontology_concepts)

        for concept in ontology_concepts:
            term = concept['term']
            term_index = tmp_text.index(term) - 1
            prev_char = tmp_text[term_index:term_index + 1]
            # print('term: %s, prev_char: %s' % (term, prev_char))
            tmp_text = tmp_text.replace(term, self.REMOVED_TAG)
            concept['operator'] = prev_char if prev_char == '-' else ''

        # print(tmp_text)

        skills = [c['concept'].lower() for c in ontology_concepts
                  if self._filter_concepts(c, self.COMPETENCE_KEY, '')]
        occupations = [c['concept'].lower() for c in ontology_concepts
                       if self._filter_concepts(c, self.OCCUPATION_KEY, '')]
        traits = [c['concept'].lower() for c in ontology_concepts
                  if self._filter_concepts(c, self.TRAIT_KEY, '')]

        skills_must_not = [c['concept'].lower() for c in ontology_concepts
                           if self._filter_concepts(c, self.COMPETENCE_KEY, '-')]
        occupations_must_not = [c['concept'].lower() for c in ontology_concepts
                                if self._filter_concepts(c, self.OCCUPATION_KEY, '-')]
        traits_must_not = [c['concept'].lower() for c in ontology_concepts
                           if self._filter_concepts(c, self.TRAIT_KEY, '-')]

        result = {'skills': skills,
                  'occupations': occupations,
                  'traits': traits,
                  'skills_must_not': skills_must_not,
                  'occupations_must_not': occupations_must_not,
                  'traits_must_not': traits_must_not}

        return result

    def _get_concepts(self, text, concept_type=None, span_info=False):
        concepts = self.keyword_processor.extract_keywords(text, span_info=span_info)
        if concept_type is not None:
            if span_info:
                concepts = list(filter(lambda concept: concept[0]['type'] == concept_type,
                                       concepts))
            else:
                concepts = list(filter(lambda concept: concept['type'] == concept_type,
                                       concepts))
        print('Returning concepts', concepts)
        return concepts

    def _filter_concepts(self, concept, concept_type, operator):
        if concept['type'] == concept_type and concept['operator'] == operator:
            return True
        else:
            return False
