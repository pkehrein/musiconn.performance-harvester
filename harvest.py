import copy
import os
import requests
import json
import rdflib
from rdflib import Graph, plugin
from rdflib.plugin import register, Serializer
from rdflib import URIRef, Literal, Namespace, BNode
from rdflib.namespace import RDF, RDFS, OWL, SDO, XSD
from datetime import date


location_auth = {}
series_auth = {}
source_auth = {}
person_auth = {}
subject_auth = {}
corporation_auth = {}
work_auth = {}
event_auth = {}
save_meta = False
N4C = Namespace("https://nfdi4culture.de/id/")
CTO = Namespace("https://nfdi4culture.de/ontology#")
NFDICORE = Namespace("https://nfdi.fiz-karlsruhe.de/ontology#")


def init_graph():
    graph = Graph()
    graph.remove((None, None, None))
    graph.bind("cto", CTO)
    graph.bind("nfdicore", NFDICORE)
    graph.bind("n4c", N4C)
    graph.add((N4C.E5378, SDO.dateModified, Literal(date.today(), datatype=XSD.date)))

    return graph


def add_events(events):
    graph = init_graph()
    for event in events:
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
        eventdate = event['schema:event']['schema:temporalCoverage']['@value']
        startdate = eventdate[:eventdate.index('/')]
        enddate = eventdate[(eventdate.index('/') + 1):]
        graph.add((event_id, NFDICORE.startDate, Literal(startdate)))
        graph.add((event_id, NFDICORE.endDate, Literal(enddate)))

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

        for works in event['schema:event']['schema:workPerformed']:
            for work in works['@id']:
                if works['@id'][work]['gnd'] is not None:
                    graph.add((event_id, CTO.relatedItem, URIRef(works['@id'][work]['gnd'])))
                    graph.add((event_id, CTO.gnd, URIRef(works['@id'][work]['gnd'])))
                if works['@id'][work]['viaf'] is not None:
                    graph.add((event_id, CTO.relatedItem, URIRef(works['@id'][work]['viaf'])))
                    graph.add((event_id, CTO.viaf, URIRef(works['@id'][work]['viaf'])))
                if works['@id'][work]['gnd'] is None and works['@id'][work]['viaf'] is None:
                    graph.add((event_id, CTO.relatedItem, URIRef(work)))

    graph.serialize(destination='events.ttl')


def parse_category_sizes(header):
    global event_count
    global performer_count
    global work_count
    global composer_count
    global person_count
    global series_count
    global corporation_count
    global location_count
    global source_count

    event_count = header['count']['event']
    # performer_count = header['count']['performer']
    work_count = header['count']['work']
    # composer_count = header['count']['composer']
    person_count = header['count']['person']
    series_count = header['count']['series']
    corporation_count = header['count']['corporation']
    location_count = header['count']['location']
    source_count = header['count']['source']


def fetch_json_data(url):
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers)
    if response.status_code == 200:
        try:
            return response.json()
        except json.JSONDecodeError:
            print(f"Failed to decode json")
            return None
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None


def harvest_category(category_count, category):
    category_container = []
    for i in range(1, category_count + 1):
        data = fetch_json_data(f"https://performance.musiconn.de/api?action=get&format=json&{category}={str(i)}")
        if data is not None:
            category_container.append(data)
            print(f"Harvested Item " + str(i) + " for Category: " + category)
    return category_container


def load_template(category):
    with open(f'templates/template_{category}.json', 'r', encoding='utf-8') as file:
        return json.load(file)


def save_json_category_data(category_data, file_path):
    for index, data in enumerate(category_data):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(f"{file_path}{str(index + 1).zfill(5)}.json", 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)
            print(f"\nData saved to {file_path}")


def map_json_data(data, template, index):
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
        map_event(data, template, index)

    if "work" in data:
        map_work(data, template, index)

    return template


def map_event(data, template, index):
    global save_meta
    data_prefix = data['event'][str(index + 1)]
    template_prefix = template['schema:event']
    # template['schema:event']['@id'] = generate_id(data)
    template_prefix['schema:name'] = data_prefix['title']
    template_prefix['schema:temporalCoverage']['@value'] = parse_time(data_prefix)
    location_index = data_prefix['locations'][0]['location']
    if str(location_index) not in location_auth:
        location_auth[f'{location_index}'] = fetch_meta_data(location_index, 'location')
        save_meta = True
    template_prefix['schema:location'] = location_auth[f'{location_index}']
    if data_prefix['names'] is not None:
        template_prefix['schema:alternateName'] = enrich_names(data_prefix)
    series_index = data_prefix['serials'][0]['series']
    if str(series_index) not in series_auth:
        series_auth[f'{series_index}'] = fetch_meta_data(series_index, 'series')
        save_meta = True
    template_prefix['schema:superEvent'][0]['@id'] = series_auth[f'{series_index}']
    if data_prefix['sources'] is not None:
        sources = []
        for index, source in enumerate(data_prefix['sources']):
            source_index = data_prefix['sources'][index]['source']
            if str(source_index) not in source_auth:
                source_auth[f'{source_index}'] = fetch_meta_data(source_index, 'source')
                save_meta = True
            sources.append({'@id': copy.deepcopy(source_auth[f'{source_index}'])})
        template_prefix['schema:recordedIn'] = sources
    if data_prefix['persons'] is not None or data_prefix['corporations'] is not None:
        template_prefix['schema:performer'] = complete_event_performers(data_prefix, True)

    if data_prefix['performances'] is not None:
        works = []
        for index, work in enumerate(data_prefix['performances']):
            work_index = data_prefix['performances'][index]['work']
            if str(work_index) not in work_auth:
                work_auth[f'{work_index}'] = fetch_meta_data(work_index, 'work')
                save_meta = True
            composers = []
            for comp_index, composer in enumerate(data_prefix['performances'][index]['composers']):
                composer_index = data_prefix['performances'][index]['composers'][comp_index]['person']
                if str(composer_index) not in person_auth:
                    person_auth[f'{composer_index}'] = fetch_meta_data(composer_index, 'person')
                    save_meta = True
                composers.append({"@type": "schema:Person", '@id': copy.deepcopy(person_auth[f'{composer_index}'])})
            works.append({'@id': work_auth[f'{work_index}'], 'schema:author': composers})
        template_prefix['schema:workPerformed'] = works
    if data_prefix['url'] is not None:
        template_prefix['@id'] = data_prefix['url']


def enrich_names(data_prefix):
    names = []
    for name in data_prefix['names']:
        names.append({'@value': name['name']})
    return names


def map_work(data, template, index):
    global save_meta
    data_prefix = data['work'][str(index+1)]
    template_prefix = template['schema:MusicComposition']
    template_prefix['schema:name'] = data_prefix['title']
    if data_prefix['names'] is not None:
        template_prefix['schema:alternateName'] = enrich_names(data_prefix)
    if data_prefix['persons'] is not None or data_prefix['corporations'] is not None:
        template_prefix['schema:contributor'] = complete_event_performers(data_prefix, False)
    if data_prefix['url'] is not None:
        template_prefix['@id'] = data_prefix['url']
    if data_prefix['genres'] is not None:
        genres = []
        for index, genre in enumerate(data_prefix['genres']):
            genre_index = data_prefix['genres'][index]['subject']
            if str(genre_index) not in subject_auth:
                subject_auth[f'{genre_index}'] = fetch_meta_data(genre_index, 'subject')
                save_meta = True
            genres.append({'@id': copy.deepcopy(subject_auth[f'{genre_index}'])})
        template_prefix['schema:genre'] = genres
    if 'descriptions' in data_prefix and data_prefix['descriptions'] is not None:
        descriptions = []
        for description in data_prefix['descriptions']:
            descriptions.append({'@value': copy.deepcopy(description['description'])})
        template_prefix['schema:description'] = descriptions
    else:
        template_prefix['schema:description'] = []
    if 'childs' in data_prefix and data_prefix['childs'] is not None:
        childs = []
        for index, child in enumerate(data_prefix['childs']):
            work_index = data_prefix['childs'][index]['work']
            if str(work_index) not in work_auth:
                work_auth[f'{work_index}'] = fetch_meta_data(work_index, 'work')
                save_meta = True
            childs.append({"@type": "schema:MusicComposition", 'id': copy.deepcopy(work_auth[f'{work_index}'])})
        template_prefix['schema:includedComposition'] = childs
    if 'composers' in data_prefix and data_prefix['composers'] is not None:
        composers = []
        for index, composer in enumerate(data_prefix['composers']):
            composer_index = data_prefix['composers'][index]['person']
            if str(composer_index) not in person_auth:
                person_auth[f'{composer_index}'] = fetch_meta_data(composer_index, 'person')
                save_meta = True
            composers.append({"@type": "schema:Person", "@id": copy.deepcopy(person_auth[f'{composer_index}'])})
        template_prefix['schema:composer'] = composers
    if 'events' in data_prefix and data_prefix['events'] is not None:
        events = []
        for index, event in enumerate(data_prefix['events']):
            event_index = data_prefix['events'][index]['event']
            if str(event_index) not in event_auth:
                event_auth[f'{event_index}'] = fetch_meta_data(event_index, 'event')
                save_meta = True
            events.append({'@type': 'schema:event', '@id': copy.deepcopy(event_auth[f'{event_index}'])})
        template_prefix['schema:subjectOf'] = events


def complete_event_performers(data_prefix, role):
    global save_meta
    performers = []
    for index, person in enumerate(data_prefix['persons']):
        person_index = data_prefix['persons'][index]['person']
        if str(person_index) not in person_auth:
            person_auth[f'{person_index}'] = fetch_meta_data(person_index, 'person')
            save_meta = True
        if role:
            occupation_index = data_prefix['persons'][index]['subject']
            if str(occupation_index) not in subject_auth:
                subject_auth[f'{occupation_index}'] = fetch_meta_data(occupation_index, 'subject')
                save_meta = True
            performers.append({'@type': 'schema:Person', '@id': copy.deepcopy(person_auth[f'{person_index}']),
                               'schema:hasOccupation': copy.deepcopy(subject_auth[f'{occupation_index}'])})
        else:
            performers.append({'@type': 'schema:Person', '@id': copy.deepcopy(person_auth[f'{person_index}'])})
    for index, corporation in enumerate(data_prefix['corporations']):
        corporation_index = data_prefix['corporations'][index]['corporation']
        if str(corporation_index) not in corporation_auth:
            corporation_auth[f'{corporation_index}'] = fetch_meta_data(corporation_index, 'corporation')
            save_meta = True
        if role:
            occupation_index = data_prefix['corporations'][index]['subject']
            if str(occupation_index) not in subject_auth:
                subject_auth[f'{occupation_index}'] = fetch_meta_data(occupation_index, 'subject')
                save_meta = True
            performers.append(
                {'@type': 'schema:PerformingGroup', '@id': copy.deepcopy(corporation_auth[f'{corporation_index}']),
                 'schema:description': copy.deepcopy(subject_auth[f'{occupation_index}'])})
        else:
            performers.append({'@type': 'schema:PerformingGroup', '@id': copy.deepcopy(corporation_auth[f'{corporation_index}'])})
    return performers


def parse_time(item):
    first_date = item['dates'][0]['date'] if len(item['dates']) != 0 else 0
    if first_date == 0: return None
    if len(item['dates']) > 1:
        last_date = item['dates'][-1]
    else:
        last_date = first_date
    time = f"{first_date}T{item['times'][0]['time']}/{last_date}T{item['times'][-1]['time']}"
    return time


def generate_id(item):
    return None


def process_json_data():
    header = fetch_json_data("https://performance.musiconn.de/api?action=query&format=json&entity=null")
    parse_category_sizes(header)

    event_template = load_template('event')
    events = harvest_category(10, "event")
    mapped_events = []
    for index, event in enumerate(events):
        event = map_json_data(event, event_template, index)
        mapped_events.append(copy.deepcopy(event))
    save_json_category_data(mapped_events, f"event_feed/")
    add_events(mapped_events)

    work_template = load_template('work')
    works = harvest_category(10, 'work')
    mapped_work = []
    for index, work in enumerate(works):
        work = map_json_data(work, work_template, index)
        mapped_work.append(copy.deepcopy(work))
    save_json_category_data(mapped_work, f'work_feed/')

    if save_meta:
        save_meta_data_to_json(location_auth, 'authorities/location.json')
        save_meta_data_to_json(series_auth, 'authorities/series.json')
        save_meta_data_to_json(source_auth, 'authorities/sources.json')
        save_meta_data_to_json(person_auth, 'authorities/persons.json')
        save_meta_data_to_json(subject_auth, 'authorities/subjects.json')
        save_meta_data_to_json(corporation_auth, 'authorities/corporations.json')
        save_meta_data_to_json(work_auth, 'authorities/works.json')
        save_meta_data_to_json(event_auth, 'authorities/events.json')


def fetch_meta_data(index, category):
    authority_linked = {}
    data = fetch_json_data(f"https://performance.musiconn.de/api?action=get&format=json&{category}={str(index)}")
    link = data[category][str(index)]['url']
    authority = {"gnd": None, "viaf": None}
    data_prefix = data[category][str(index)]
    if "authorities" in data_prefix and data_prefix["authorities"] is not None:
        authorities = fetch_authorities(data_prefix["authorities"])
        for item in authorities:
            authority_link = item['url']
            if "gnd" in authority_link:
                authority["gnd"] = authority_link
            if "viaf" in authority_link:
                authority["viaf"] = authority_link
    authority_linked[link] = authority
    return authority_linked


def fetch_authorities(authority_list):
    data_list = []
    for authority in authority_list:
        auth_data = fetch_json_data(f"https://performance.musiconn.de/api?action=get&format=json&authority={authority['authority']}")
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
    load_meta_data()
    process_json_data()

