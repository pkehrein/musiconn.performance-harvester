import copy
import json
import os
import re
import time
from argparse import ArgumentParser
from datetime import date

import requests
from rdflib import Graph
from rdflib import URIRef, Literal, Namespace, BNode
from rdflib.namespace import RDF, SDO, XSD

location_auth = {}
series_auth = {}
source_auth = {}
person_auth = {}
subject_auth = {}
corporation_auth = {}
work_auth = {}
event_auth = {}
event_count = 0
work_count = 0
save_meta = False
N4C = Namespace("https://nfdi4culture.de/id/")
CTO = Namespace("https://nfdi4culture.de/ontology#")
NFDICORE = Namespace("https://nfdi.fiz-karlsruhe.de/ontology#")


def concat_files():
    header = True
    count = 0
    with open("feed.ttl", 'w', encoding='utf-8') as file:
        for filename in os.listdir('event_result/'):
            filepath = os.path.join('event_result/', filename)
            with open(filepath, 'r', encoding='utf-8') as readfile:
                if header:
                    modified_content = readfile.read()
                    header = False
                else:
                    content = readfile.read()
                    modified_content = remove_header(content)
                file.write(modified_content + '\n')
            count += 1
        for filename in os.listdir('work_result/'):
            filepath = os.path.join('work_result/', filename)
            with open(filepath, 'r', encoding='utf-8') as readfile:
                file.write((remove_header(readfile.read())) + '\n')
            count += 1
    print(f"##### Finished concatenating {count} turtle-files into feed.ttl #####")


def remove_header(content):
    modified_content = re.sub('@.*\n', "", content)
    return re.sub('.*schema:dateModified.*\n', "", modified_content)


def init_graph():
    graph = Graph()
    graph.remove((None, None, None))
    graph.bind("cto", CTO)
    graph.bind("nfdicore", NFDICORE)
    graph.bind("n4c", N4C)
    return graph


def add_events(events, file_path, start_index):
    for event in events:
        graph = init_graph()
        event_id = URIRef(event['schema:event']['@id'])
        bn = BNode()
        graph.add((N4C.E5320, SDO.dataFeedElement, bn))
        graph.add((bn, RDF.type, SDO.DataFeedItem))
        graph.add((bn, SDO.item, event_id))
        graph.add((event_id, RDF.type, CTO.DataFeedElement))
        graph.add((event_id, RDF.type, NFDICORE.Event))
        graph.add((event_id, CTO.elementType, URIRef("http://vocab.getty.edu/aat/300069451")))
        graph.add((event_id, NFDICORE.publisher, URIRef("https://nfdi4culture.de/id/E1841")))
        graph.add((event_id, CTO.elementOf, URIRef("https://nfdi4culture.de/id/E5320")))
        graph.add((event_id, CTO.title, Literal(event['schema:event']['schema:name'])))
        if event['schema:event']['schema:temporalCoverage']['@value'] is not None:
            eventdate = event['schema:event']['schema:temporalCoverage']['@value']
            startdate = eventdate[:eventdate.index('/')]
            enddate = eventdate[(eventdate.index('/') + 1):]
            graph.add((event_id, NFDICORE.startDate, Literal(startdate)))
            graph.add((event_id, NFDICORE.endDate, Literal(enddate)))

        if event['schema:event']['schema:location'] is not None:
            location = event['schema:event']['schema:location']
            for loc in location:
                if location[loc]['gnd'] is not None:
                    graph.add((event_id, CTO.relatedLocation, URIRef(location[loc]['gnd'])))
                    graph.add((event_id, CTO.gnd, URIRef(location[loc]['gnd'])))
                if location[loc]['viaf'] is not None:
                    graph.add((event_id, CTO.relatedLocation, URIRef(location[loc]['viaf'])))
                    graph.add((event_id, CTO.viaf, URIRef(location[loc]['viaf'])))
                if location[loc]['gnd'] is None and location[loc]['viaf'] is None:
                    graph.add((event_id, CTO.relatedLocation, URIRef(loc)))

        if event['schema:event']['schema:superEvent'] is not None:
            for superEvent in event['schema:event']['schema:superEvent']:
                series = superEvent['@id']
                for ser in series:
                    if series[ser]['gnd'] is not None:
                        graph.add((event_id, CTO.itemOf, URIRef(series[ser]['gnd'])))
                        graph.add((event_id, CTO.gnd, URIRef(series[ser]['gnd'])))
                    if series[ser]['viaf'] is not None:
                        graph.add((event_id, CTO.itemOf, URIRef(series[ser]['viaf'])))
                        graph.add((event_id, CTO.viaf, URIRef(series[ser]['viaf'])))
                    if series[ser]['gnd'] is None and series[ser]['viaf'] is None:
                        graph.add((event_id, CTO.itemOf, URIRef(ser)))

        if event['schema:event']['schema:recordedIn'] is not None:
            for record in event['schema:event']['schema:recordedIn']:
                source = record['@id']
                for sou in source:
                    if source[sou]['gnd'] is not None:
                        graph.add((event_id, CTO.relatedItem, URIRef(source[sou]['gnd'])))
                        graph.add((event_id, CTO.gnd, URIRef(source[sou]['gnd'])))
                    if source[sou]['viaf'] is not None:
                        graph.add((event_id, CTO.relatedItem, URIRef(source[sou]['viaf'])))
                        graph.add((event_id, CTO.viaf, URIRef(source[sou]['viaf'])))
                    if source[sou]['gnd'] is None and source[sou]['viaf'] is None:
                        graph.add((event_id, CTO.relatedItem, URIRef(sou)))

        if event['schema:event']['schema:performer'] is not None:
            for performer in event['schema:event']['schema:performer']:
                if performer['@type'] == 'schema:Person':
                    for person in performer['@id']:
                        if performer['@id'][person]['gnd'] is not None:
                            graph.add((event_id, CTO.relatedPerson, URIRef(performer['@id'][person]['gnd'])))
                            graph.add((event_id, CTO.gnd, URIRef(performer['@id'][person]['gnd'])))
                        if performer['@id'][person]['viaf'] is not None:
                            graph.add((event_id, CTO.relatedPerson, URIRef(performer['@id'][person]['viaf'])))
                            graph.add((event_id, CTO.viaf, URIRef(performer['@id'][person]['viaf'])))
                        if performer['@id'][person]['gnd'] is None and performer['@id'][person]['viaf'] is None:
                            graph.add((event_id, CTO.relatedPerson, URIRef(person)))
                if performer['@type'] == 'schema:PerformingGroup':
                    for group in performer['@id']:
                        if performer['@id'][group]['gnd'] is not None:
                            graph.add((event_id, CTO.relatedOrganization, URIRef(performer['@id'][group]['gnd'])))
                            graph.add((event_id, CTO.gnd, URIRef(performer['@id'][group]['gnd'])))
                        if performer['@id'][group]['viaf'] is not None:
                            graph.add((event_id, CTO.relatedOrganization, URIRef(performer['@id'][group]['viaf'])))
                            graph.add((event_id, CTO.viaf, URIRef(performer['@id'][group]['viaf'])))
                        if performer['@id'][group]['gnd'] is None and performer['@id'][group]['viaf'] is None:
                            graph.add((event_id, CTO.relatedOrganization, URIRef(group)))
        if event['schema:event']['schema:workPerformed'] is not None:
            for works in event['schema:event']['schema:workPerformed']:
                for work in works['@id']:
                    graph.add((event_id, CTO.relatedItem, URIRef(work)))
                    if works['@id'][work]['gnd'] is not None:
                        graph.add((event_id, CTO.relatedItem, URIRef(works['@id'][work]['gnd'])))
                        graph.add((event_id, CTO.gnd, URIRef(works['@id'][work]['gnd'])))
                    if works['@id'][work]['viaf'] is not None:
                        graph.add((event_id, CTO.relatedItem, URIRef(works['@id'][work]['viaf'])))
                        graph.add((event_id, CTO.viaf, URIRef(works['@id'][work]['viaf'])))
                    if works['@id'][work]['gnd'] is None and works['@id'][work]['viaf'] is None:
                        graph.add((event_id, CTO.relatedItem, URIRef(work)))
        turtle_data = graph.serialize(format='turtle')
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(f"{file_path}{str(start_index + 1).zfill(5)}.ttl", 'w', encoding='utf-8') as file:
            file.write(turtle_data)
        print(f"Saved turtle data to {file_path}{str(start_index + 1).zfill(5)}.ttl")
        start_index += 1


def add_works(works, file_path, start_index):
    for work in works:
        graph = init_graph()
        work_id = URIRef(work['schema:MusicComposition']['@id'])
        bn = BNode()
        graph.add((N4C.E5320, SDO.dataFeedElement, bn))
        graph.add((bn, RDF.type, SDO.DataFeedItem))
        graph.add((bn, SDO.item, work_id))
        graph.add((work_id, RDF.type, CTO.DataFeedElement))
        graph.add((work_id, RDF.type, SDO.MusicComposition))
        graph.add((work_id, NFDICORE.publisher, URIRef("https://nfdi4culture.de/id/E1841")))
        graph.add((work_id, CTO.elementOf, URIRef("https://nfdi4culture.de/id/E5320")))
        graph.add((work_id, CTO.title, Literal(work['schema:MusicComposition']['schema:name'])))

        composers = work['schema:MusicComposition']['schema:composer']
        for composer in composers:
            for comp_item in composer['@id']:
                if composer['@id'][comp_item]['gnd'] is not None:
                    graph.add((work_id, CTO.relatedPerson, URIRef(composer['@id'][comp_item]['gnd'])))
                    graph.add((work_id, CTO.gnd, URIRef((composer['@id'][comp_item]['gnd']))))
                if composer['@id'][comp_item]['viaf'] is not None:
                    graph.add((work_id, CTO.relatedPerson, URIRef(composer['@id'][comp_item]['viaf'])))
                    graph.add((work_id, CTO.viaf, URIRef(composer['@id'][comp_item]['viaf'])))
                if composer['@id'][comp_item]['gnd'] is None and composer['@id'][comp_item]['viaf'] is None:
                    graph.add((work_id, CTO.relatedPerson, URIRef(comp_item)))
        descriptions = work['schema:MusicComposition']['schema:description']
        if descriptions is not None:
            for desc_index, description in enumerate(descriptions):
                graph.add((work_id, CTO.abstract, Literal(descriptions[desc_index]['@value'])))
        genres = work['schema:MusicComposition']['schema:genre']
        if genres is not None:
            for genre in genres:
                for genre_item in genre['@id']:
                    if genre['@id'][genre_item]['gnd'] is not None:
                        graph.add((work_id, CTO.isPartOf, URIRef(genre['@id'][genre_item]['gnd'])))
                        graph.add((work_id, CTO.gnd, URIRef(genre['@id'][genre_item]['gnd'])))
                    if genre['@id'][genre_item]['viaf'] is not None:
                        graph.add((work_id, CTO.isPartOf, URIRef(genre['@id'][genre_item]['viaf'])))
                        graph.add((work_id, CTO.viaf, URIRef(genre['@id'][genre_item]['viaf'])))
                    if genre['@id'][genre_item]['gnd'] is None and genre['@id'][genre_item]['viaf'] is None:
                        graph.add((work_id, CTO.isPartOf, URIRef(genre_item)))

        compositions = work['schema:MusicComposition']['schema:includedComposition']
        for comp_index, composition in enumerate(compositions):
            for composition_item in compositions[comp_index]['@id']:
                graph.add((work_id, SDO.includedComposition, URIRef(composition_item)))
                if compositions[comp_index]['@id'][composition_item]['gnd'] is not None:
                    graph.add((work_id, SDO.includedComposition,
                               URIRef(compositions[comp_index]['@id'][composition_item]['gnd'])))
                    graph.add((work_id, CTO.gnd, URIRef(compositions[comp_index]['@id'][composition_item]['gnd'])))
                if compositions[comp_index]['@id'][composition_item]['viaf'] is not None:
                    graph.add((work_id, SDO.includedComposition,
                               URIRef(compositions[comp_index]['@id'][composition_item]['viaf'])))
                    graph.add((work_id, CTO.viaf, URIRef(compositions[comp_index]['@id'][composition_item]['viaf'])))

        events = work['schema:MusicComposition']['schema:subjectOf']
        if events is not None:
            for event in events:
                for event_item in event['@id']:
                    graph.add((work_id, SDO.subjectOf, URIRef(event_item)))
                    if event['@id'][event_item]['gnd'] is not None:
                        graph.add((work_id, SDO.subjectOf, URIRef(event['@id'][event_item]['gnd'])))
                        graph.add((work_id, CTO.gnd, URIRef(event['@id'][event_item]['gnd'])))
                    if event['@id'][event_item]['viaf'] is not None:
                        graph.add((work_id, SDO.subjectOf, URIRef(event['@id'][event_item]['viaf'])))
                        graph.add((work_id, CTO.viaf, URIRef(event['@id'][event_item]['viaf'])))

        contributors = work['schema:MusicComposition']['schema:contributor']
        if contributors is not None:
            for contributor in contributors:
                if contributor['@type'] == 'schema:Person':
                    for person in contributor['@id']:
                        if contributor['@id'][person]['gnd'] is not None:
                            graph.add((work_id, CTO.relatedPerson, URIRef(contributor['@id'][person]['gnd'])))
                            graph.add((work_id, CTO.gnd, URIRef(contributor['@id'][person]['gnd'])))
                        if contributor['@id'][person]['viaf'] is not None:
                            graph.add((work_id, CTO.relatedPerson, URIRef(contributor['@id'][person]['viaf'])))
                            graph.add((work_id, CTO.viaf, URIRef(contributor['@id'][person]['viaf'])))
                        if contributor['@id'][person]['gnd'] is None and contributor['@id'][person]['viaf'] is None:
                            graph.add((work_id, CTO.relatedPerson, URIRef(person)))
                if contributor['@type'] == 'schema:PerformingGroup':
                    for group in contributor['@id']:
                        if contributor['@id'][group]['gnd'] is not None:
                            graph.add((work_id, CTO.relatedOrganization, URIRef(contributor['@id'][group]['gnd'])))
                            graph.add((work_id, CTO.gnd, URIRef(contributor['@id'][group]['gnd'])))
                        if contributor['@id'][group]['viaf'] is not None:
                            graph.add((work_id, CTO.relatedOrganization, URIRef(contributor['@id'][group]['viaf'])))
                            graph.add((work_id, CTO.viaf, URIRef(contributor['@id'][group]['viaf'])))
                        if contributor['@id'][group]['gnd'] is None and contributor['@id'][group]['viaf'] is None:
                            graph.add((work_id, CTO.relatedOrganization, URIRef(group)))
        turtle_data = graph.serialize(format='turtle')
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(f"{file_path}{str(start_index + 1).zfill(5)}.ttl", 'w', encoding='utf-8') as file:
            file.write(turtle_data)
        print(f"Saved turtle data to {file_path}{str(start_index + 1).zfill(5)}.ttl")
        start_index += 1


def parse_category_sizes(header):
    global event_count
    global work_count

    event_count = header['count']['event']
    work_count = header['count']['work']


def parse_arguments():
    parser = ArgumentParser(description="Harvest data from the musiconn.performance-API and map it to JSON-LD and ttl")
    parser.add_argument('-w', '--wait', type=float, default=1, help="Wait time in between API-Requests")
    parser.add_argument('-c', '--count', type=int, default=0,
                        help="Number of items to be harvested. Default is all items (Warning: This might take a long "
                             "time!)")
    parser.add_argument('-a', '--startIndexEvent', type=int, default=0,
                        help="Index of the first Event to be harvested. Default is 0")
    parser.add_argument('-b', '--startIndexWork', type=int, default=0, help="Index of the first work to be harvested. "
                                                                            "Default is 0")
    parser.add_argument('-F', '--singleFile', action='store_true',
                        help="Save all items in one graph and ttl-File.")
    parser.add_argument('-e', '--loadEvents', action='store_true', help="Load cached events to transform to "
                                                                        "turtle instead of harvesting them first")
    parser.add_argument('-l', '--loadWorks', action='store_true', help="Load cached works to transform to turtle "
                                                                       "instead of harvesting them first")
    parser.add_argument('-E', '--disableEvents', action='store_true', help="Disable harvesting, mapping and "
                                                                           "transformation to turtle for the "
                                                                           "item-type event")
    parser.add_argument('-W', '--disableWorks', action='store_true', help="Disable harvesting, mapping and "
                                                                          "transformation to turtle for the "
                                                                          "item-type work")
    return parser.parse_args()


def fetch_json_data(url, wait_time):
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers)
    if response.status_code == 200:
        try:
            if wait_time > 0:
                time.sleep(wait_time)
            return response.json()
        except json.JSONDecodeError:
            print(f"Failed to decode json")
            return None
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None


def harvest_category(category_count, category, wait_time, start_index):
    category_container = []
    if category_count < start_index:
        category_count = category_count + start_index
    for i in range(start_index + 1, category_count + 1):
        data = fetch_json_data(f"https://performance.musiconn.de/api?action=get&format=json&{category}={str(i)}",
                               wait_time)
        if data is not None:
            category_container.append(data)
            print(f"Harvested Item " + str(i) + " for Category: " + category)
    return category_container


def load_template(category):
    with open(f'templates/template_{category}.json', 'r', encoding='utf-8') as file:
        return json.load(file)


def save_json_data(data, file_path, index):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(f"{file_path}{str(index + 1).zfill(5)}.json", 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)
        print(f"Data saved to {file_path}{str(index + 1).zfill(5)}.json")


def map_json_data(data, template, index, wait_time):
    global location_auth
    global series_auth
    global source_auth
    global person_auth
    global subject_auth
    global corporation_auth
    global work_auth
    global event_auth
    global save_meta

    if "event" in data:
        map_event(data, template, index, wait_time)

    if "work" in data:
        map_work(data, template, index, wait_time)

    return template


def map_event(data, template, index, wait_time):
    global save_meta
    data_prefix = data['event'][str(index + 1)]
    template_prefix = template['schema:event']
    template_prefix['schema:name'] = data_prefix['title']
    if 'dates' in data_prefix:
        template_prefix['schema:temporalCoverage']['@value'] = parse_time(data_prefix)
    else:
        template_prefix['schema:temporalCoverage']['@value'] = None
    if 'locations' in data_prefix:
        location_index = data_prefix['locations'][0]['location']
        if str(location_index) not in location_auth:
            location_auth[f'{location_index}'] = fetch_meta_data(location_index, 'location', wait_time)
            save_meta = True
        template_prefix['schema:location'] = location_auth[f'{location_index}']
    else:
        template_prefix['schema:location'] = None
    if data_prefix['names'] is not None:
        template_prefix['schema:alternateName'] = enrich_names(data_prefix)
    if 'serials' in data_prefix and data_prefix['serials'] is not None:
        series_index = data_prefix['serials'][0]['series']
        if str(series_index) not in series_auth:
            series_auth[f'{series_index}'] = fetch_meta_data(series_index, 'series', wait_time)
            save_meta = True
        template_prefix['schema:superEvent'][0]['@id'] = series_auth[f'{series_index}']
    else:
        template_prefix['schema:superEvent'][0]['@id'] = {}
    if 'sources' in data_prefix and data_prefix['sources'] is not None:
        sources = []
        for sources_index, source in enumerate(data_prefix['sources']):
            source_index = data_prefix['sources'][sources_index]['source']
            if str(source_index) not in source_auth:
                source_auth[f'{source_index}'] = fetch_meta_data(source_index, 'source', wait_time)
                save_meta = True
            sources.append({'@id': copy.deepcopy(source_auth[f'{source_index}'])})
        template_prefix['schema:recordedIn'] = sources
    else:
        template_prefix['schema:recordedIn'] = []
    if 'persons' in data_prefix and data_prefix['persons'] is not None:
        template_prefix['schema:performer'] = complete_event_performers(data_prefix, True, wait_time, 'persons')
    else:
        template_prefix['schema:performer'] = []
    if 'corporations' in data_prefix and data_prefix['corporations'] is not None:
        template_prefix['schema:performer'] = complete_event_performers(data_prefix, True, wait_time, 'corporations')
    elif 'persons' not in data_prefix:
        template_prefix['schema:performer'] = []
    if 'performances' in data_prefix and data_prefix['performances'] is not None:
        works = []
        for works_index, work in enumerate(data_prefix['performances']):
            work_index = data_prefix['performances'][works_index]['work']
            if str(work_index) not in work_auth:
                work_auth[f'{work_index}'] = fetch_meta_data(work_index, 'work', wait_time)
                save_meta = True
            composers = []
            if 'composers' in data_prefix['performances'][works_index]:
                for comp_index, composer in enumerate(data_prefix['performances'][works_index]['composers']):
                    composer_index = data_prefix['performances'][works_index]['composers'][comp_index]['person']
                    if str(composer_index) not in person_auth:
                        person_auth[f'{composer_index}'] = fetch_meta_data(composer_index, 'person', wait_time)
                        save_meta = True
                    composers.append({"@type": "schema:Person", '@id': copy.deepcopy(person_auth[f'{composer_index}'])})
            works.append({'@id': work_auth[f'{work_index}'], 'schema:author': composers})
        template_prefix['schema:workPerformed'] = works
    else:
        template_prefix['schema:workPerformed'] = []
    if data_prefix['url'] is not None:
        template_prefix['@id'] = data_prefix['url']


def enrich_names(data_prefix):
    names = []
    for name in data_prefix['names']:
        names.append({'@value': name['name']})
    return names


def map_work(data, template, index, wait_time):
    global save_meta
    data_prefix = data['work'][str(index + 1)]
    template_prefix = template['schema:MusicComposition']
    template_prefix['schema:name'] = data_prefix['title']
    if data_prefix['names'] is not None:
        template_prefix['schema:alternateName'] = enrich_names(data_prefix)
    if 'persons' in data_prefix and data_prefix['persons'] is not None:
        template_prefix['schema:contributor'] = complete_event_performers(data_prefix, False, wait_time, 'persons')
    else:
        template_prefix['schema:contributor'] = []
    if 'corporations' in data_prefix and data_prefix['corporations'] is not None:
        template_prefix['schema:contributor'] = complete_event_performers(data_prefix, False, wait_time, 'corporations')
    elif 'persons' not in data_prefix:
        template_prefix['schema:contributor'] = []
    if data_prefix['url'] is not None:
        template_prefix['@id'] = data_prefix['url']
    if 'genres' in data_prefix and data_prefix['genres'] is not None:
        genres = []
        for gen_index, genre in enumerate(data_prefix['genres']):
            genre_index = data_prefix['genres'][gen_index]['subject']
            if str(genre_index) not in subject_auth:
                subject_auth[f'{genre_index}'] = fetch_meta_data(genre_index, 'subject', wait_time)
                save_meta = True
            genres.append({'@id': copy.deepcopy(subject_auth[f'{genre_index}'])})
        template_prefix['schema:genre'] = genres
    else:
        template_prefix['schema:genre'] = []
    if 'descriptions' in data_prefix and data_prefix['descriptions'] is not None:
        descriptions = []
        for description in data_prefix['descriptions']:
            descriptions.append({'@value': copy.deepcopy(description['description'])})
        template_prefix['schema:description'] = descriptions
    else:
        template_prefix['schema:description'] = []
    if 'childs' in data_prefix and data_prefix['childs'] is not None:
        childs = []
        for child_index, child in enumerate(data_prefix['childs']):
            work_index = data_prefix['childs'][child_index]['work']
            if str(work_index) not in work_auth:
                work_auth[f'{work_index}'] = fetch_meta_data(work_index, 'work', wait_time)
                save_meta = True
            childs.append({"@type": "schema:MusicComposition", '@id': copy.deepcopy(work_auth[f'{work_index}'])})
        template_prefix['schema:includedComposition'] = childs
    else:
        template_prefix['schema:includedComposition'] = []
    if 'composers' in data_prefix and data_prefix['composers'] is not None:
        composers = []
        for comp_index, composer in enumerate(data_prefix['composers']):
            composer_index = data_prefix['composers'][comp_index]['person']
            if str(composer_index) not in person_auth:
                person_auth[f'{composer_index}'] = fetch_meta_data(composer_index, 'person', wait_time)
                save_meta = True
            composers.append({"@type": "schema:Person", "@id": copy.deepcopy(person_auth[f'{composer_index}'])})
        template_prefix['schema:composer'] = composers
    if 'events' in data_prefix and data_prefix['events'] is not None:
        events = []
        for ev_index, event in enumerate(data_prefix['events']):
            event_index = data_prefix['events'][ev_index]['event']
            if str(event_index) not in event_auth:
                event_auth[f'{event_index}'] = fetch_meta_data(event_index, 'event', wait_time)
                save_meta = True
            events.append({'@type': 'schema:event', '@id': copy.deepcopy(event_auth[f'{event_index}'])})
        template_prefix['schema:subjectOf'] = events
    else:
        template_prefix['schema:subjectOf'] = []


def complete_event_performers(data_prefix, role, wait_time, category):
    global save_meta
    performers = []
    if category == 'persons':
        for index, person in enumerate(data_prefix['persons']):
            person_index = data_prefix['persons'][index]['person']
            if str(person_index) not in person_auth:
                person_auth[f'{person_index}'] = fetch_meta_data(person_index, 'person', wait_time)
                save_meta = True
            if role:
                if 'subject' in data_prefix['persons'][index]:
                    occupation_index = data_prefix['persons'][index]['subject']
                    if str(occupation_index) not in subject_auth:
                        subject_auth[f'{occupation_index}'] = fetch_meta_data(occupation_index, 'subject', wait_time)
                        save_meta = True
                        performers.append(
                            {'@type': 'schema:Person', '@id': copy.deepcopy(person_auth[f'{person_index}']),
                             'schema:hasOccupation': copy.deepcopy(subject_auth[f'{occupation_index}'])})
                else:
                    performers.append({'@type': 'schema:Person', '@id': copy.deepcopy(person_auth[f'{person_index}']),
                                       'schema:hasOccupation': None})
            else:
                performers.append({'@type': 'schema:Person', '@id': copy.deepcopy(person_auth[f'{person_index}'])})
            print(f"Successfully added performer {person_index}")
    if category == 'corporations':
        for index, corporation in enumerate(data_prefix['corporations']):
            corporation_index = data_prefix['corporations'][index]['corporation']
            if str(corporation_index) not in corporation_auth:
                corporation_auth[f'{corporation_index}'] = fetch_meta_data(corporation_index, 'corporation', wait_time)
                save_meta = True
            if role:
                if 'subject' in data_prefix['corporations'][index]:
                    occupation_index = data_prefix['corporations'][index]['subject']
                    if str(occupation_index) not in subject_auth:
                        subject_auth[f'{occupation_index}'] = fetch_meta_data(occupation_index, 'subject', wait_time)
                        save_meta = True
                        performers.append(
                            {'@type': 'schema:PerformingGroup',
                             '@id': copy.deepcopy(corporation_auth[f'{corporation_index}']),
                             'schema:description': copy.deepcopy(subject_auth[f'{occupation_index}'])})
                else:
                    performers.append(
                        {'@type': 'schema:PerformingGroup',
                         '@id': copy.deepcopy(corporation_auth[f'{corporation_index}']),
                         'schema:description': None})
            else:
                performers.append(
                    {'@type': 'schema:PerformingGroup', '@id': copy.deepcopy(corporation_auth[f'{corporation_index}'])})
            print(f"Successfully added corporation {corporation_index}")
    return performers


def parse_time(item):
    first_date = item['dates'][0]['date'] if len(item['dates']) != 0 else 0
    if first_date == 0:
        return None
    if len(item['dates']) > 1:
        last_date = item['dates'][-1]
    else:
        last_date = first_date
    if 'times' in item:
        datetime = f"{first_date}T{item['times'][0]['time']}/{last_date}T{item['times'][-1]['time']}"
    else:
        datetime = f"{first_date}/{last_date}"
    return datetime


def process_json_data(wait_time, harvest_count, start_index_event, start_index_work, single_file, load_events, load_works,
                      disable_events, disable_works):
    header = fetch_json_data("https://performance.musiconn.de/api?action=query&format=json&entity=null", wait_time)
    parse_category_sizes(header)
    if harvest_count > 0:
        harvest_count_event = harvest_count
        harvest_count_work = harvest_count
    else:
        harvest_count_event = event_count
        harvest_count_work = work_count
    if not disable_events:
        mapped_events = []
        offset = 0
        if load_events:
            if start_index_event == 0:
                offset += 1
            file_names = sorted(os.listdir('event_feed/'))
            file_index = file_names.index(f"{str(start_index_event + offset).zfill(5)}.json")
            for filename in file_names[file_index:]:
                filepath = os.path.join('event_feed/', filename)
                with open(filepath, 'r', encoding='utf-8') as readfile:
                    mapped_events.append(json.load(readfile))

        else:
            event_template = load_template('event')
            events = harvest_category(harvest_count_event, "event", wait_time, start_index_event)
            item_index = start_index_event
            for index, event in enumerate(events):
                event = map_json_data(event, event_template, item_index, wait_time)
                mapped_events.append(copy.deepcopy(event))
                print(f"Successfully mapped event {index + 1}")
                save_json_data(event, "event_feed/", item_index)
                item_index += 1
        add_events(mapped_events, "event_result/", start_index_event)
        print(f"########## Finished harvesting and mapping category event ##########")

    if not disable_works:
        mapped_work = []
        offset = 0
        if load_works:
            if start_index_work == 0:
                offset += 1
            file_names = sorted(os.listdir('work_feed/'))
            file_index = file_names.index(f"{str(start_index_work + offset).zfill(5)}.json")
            for filename in file_names[file_index:]:
                filepath = os.path.join('work_feed/', filename)
                with open(filepath, 'r', encoding='utf-8') as readfile:
                    mapped_work.append(json.load(readfile))
        else:
            work_template = load_template('work')
            works = harvest_category(harvest_count_work, 'work', wait_time, start_index_work)
            item_index = start_index_work
            for index, work in enumerate(works):
                work = map_json_data(work, work_template, item_index, wait_time)
                mapped_work.append(copy.deepcopy(work))
                print(f"Successfully mapped work {index + 1}")
                save_json_data(work, f'work_feed/', item_index)
                item_index += 1
        add_works(mapped_work, "work_result/", start_index_work)
        print(f"########## Finished harvesting and mapping category work ##########")

    if single_file:
        concat_files()

    if save_meta:
        save_meta_data_to_json(location_auth, 'authorities/location.json')
        save_meta_data_to_json(series_auth, 'authorities/series.json')
        save_meta_data_to_json(source_auth, 'authorities/sources.json')
        save_meta_data_to_json(person_auth, 'authorities/persons.json')
        save_meta_data_to_json(subject_auth, 'authorities/subjects.json')
        save_meta_data_to_json(corporation_auth, 'authorities/corporations.json')
        save_meta_data_to_json(work_auth, 'authorities/works.json')
        save_meta_data_to_json(event_auth, 'authorities/events.json')


def fetch_meta_data(index, category, wait_time):
    authority_linked = {}
    data = fetch_json_data(f"https://performance.musiconn.de/api?action=get&format=json&{category}={str(index)}",
                           wait_time)
    if 'url' in data[category][str(index)]:
        link = data[category][str(index)]['url']
        authority = {"gnd": None, "viaf": None}
        data_prefix = data[category][str(index)]
        if "authorities" in data_prefix and data_prefix["authorities"] is not None:
            authorities = fetch_authorities(data_prefix["authorities"], wait_time)
            for item in authorities:
                authority_link = item['url']
                if "gnd" in authority_link:
                    authority["gnd"] = authority_link
                if "viaf" in authority_link:
                    authority["viaf"] = authority_link
        authority_linked[link] = authority
    return authority_linked


def fetch_authorities(authority_list, wait_time):
    data_list = []
    for authority in authority_list:
        auth_data = fetch_json_data(
            f"https://performance.musiconn.de/api?action=get&format=json&authority={authority['authority']}", wait_time)
        if auth_data:
            auth_link = auth_data["authority"][str(authority['authority'])]['links'][0]
            data_list.append(auth_link)
    return data_list


def save_meta_data_to_json(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(f"{file_path}", 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)
        print(f"\nData saved to {file_path}")


def load_meta_data():
    global location_auth
    global series_auth
    global source_auth
    global person_auth
    global subject_auth
    global corporation_auth
    global work_auth
    global event_auth

    if os.path.exists('authorities/location.json'):
        with open('authorities/location.json', 'r', encoding='utf-8') as file:
            location_auth = json.load(file)

    if os.path.exists('authorities/series.json'):
        with open('authorities/series.json', 'r', encoding='utf-8') as file:
            series_auth = json.load(file)

    if os.path.exists('authorities/sources.json'):
        with open('authorities/sources.json', 'r', encoding='utf-8') as file:
            source_auth = json.load(file)

    if os.path.exists('authorities/persons.json'):
        with open('authorities/persons.json', 'r', encoding='utf-8') as file:
            person_auth = json.load(file)

    if os.path.exists('authorities/subjects.json'):
        with open('authorities/subjects.json', 'r', encoding='utf-8') as file:
            subject_auth = json.load(file)

    if os.path.exists('authorities/corporations.json'):
        with open('authorities/corporations.json', 'r', encoding='utf-8') as file:
            corporation_auth = json.load(file)

    if os.path.exists('authorities/works.json'):
        with open('authorities/works.json', 'r', encoding='utf-8') as file:
            work_auth = json.load(file)

    if os.path.exists('authorities/events.json'):
        with open('authorities/events.json', 'r', encoding='utf-8') as file:
            event_auth = json.load(file)


if __name__ == "__main__":
    args = parse_arguments()

    load_meta_data()
    process_json_data(args.wait, args.count, args.startIndexEvent, args.startIndexWork, args.singleFile,
                      args.loadEvents, args.loadWorks, args.disableEvents, args.disableWorks)
