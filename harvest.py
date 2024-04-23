import copy
import os

import requests
import json

location_links = {}
series_links = {}
source_links = {}
person_links = {}
subject_links = {}
corporation_links = {}
work_links = {}
event_auth = {}
save_meta = False


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
    global location_links
    global series_links
    global source_links
    global person_links
    global subject_links
    global corporation_links
    global work_links
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
    if str(location_index) not in location_links:
        location_links[f'{location_index}'] = fetch_meta_data(location_index, 'location')
        save_meta = True
    template_prefix['schema:location'] = location_links[f'{location_index}']
    if data_prefix['names'] is not None:
        template_prefix['schema:alternateName'] = enrich_names(data_prefix)
    series_index = data_prefix['serials'][0]['series']
    if str(series_index) not in series_links:
        series_links[f'{series_index}'] = fetch_meta_data(series_index, 'series')
        save_meta = True
    template_prefix['schema:superEvent'][0]['@id'] = series_links[f'{series_index}']
    if data_prefix['sources'] is not None:
        sources = []
        for index, source in enumerate(data_prefix['sources']):
            source_index = data_prefix['sources'][index]['source']
            if str(source_index) not in source_links:
                source_links[f'{source_index}'] = fetch_meta_data(source_index, 'source')
                save_meta = True
            sources.append({'@id': copy.deepcopy(source_links[f'{source_index}'])})
        template_prefix['schema:recordedIn'] = sources
    if data_prefix['persons'] is not None or data_prefix['corporations'] is not None:
        template_prefix['schema:performer'] = enrich_performers(data_prefix, True)

    if data_prefix['performances'] is not None:
        works = []
        for index, work in enumerate(data_prefix['performances']):
            work_index = data_prefix['performances'][index]['work']
            if str(work_index) not in work_links:
                work_links[f'{work_index}'] = fetch_meta_data(work_index, 'work')
                save_meta = True
            composers = []
            for comp_index, composer in enumerate(data_prefix['performances'][index]['composers']):
                composer_index = data_prefix['performances'][index]['composers'][comp_index]['person']
                if str(composer_index) not in person_links:
                    person_links[f'{composer_index}'] = fetch_meta_data(composer_index, 'person')
                    save_meta = True
                composers.append({"@type": "schema:Person", '@id': copy.deepcopy(person_links[f'{composer_index}'])})
            works.append({'@id': work_links[f'{work_index}'], 'schema:author': composers})
        template_prefix['schema:workPerformed'] = works
    if data_prefix['url'] is not None:
        template_prefix['schema:url'] = data_prefix['url']


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
        template_prefix['schema:contributor'] = enrich_performers(data_prefix, False)
    if data_prefix['url'] is not None:
        template_prefix['schema:url'] = data_prefix['url']
    if data_prefix['genres'] is not None:
        genres = []
        for index, genre in enumerate(data_prefix['genres']):
            genre_index = data_prefix['genres'][index]['subject']
            if str(genre_index) not in subject_links:
                subject_links[f'{genre_index}'] = fetch_meta_data(genre_index, 'subject')
                save_meta = True
            genres.append({'@id': copy.deepcopy(subject_links[f'{genre_index}'])})
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
            if str(work_index) not in work_links:
                work_links[f'{work_index}'] = fetch_meta_data(work_index, 'work')
                save_meta = True
            childs.append({"@type": "schema:MusicComposition", 'id': copy.deepcopy(work_links[f'{work_index}'])})
        template_prefix['schema:includedComposition'] = childs
    if 'composers' in data_prefix and data_prefix['composers'] is not None:
        composers = []
        for index, composer in enumerate(data_prefix['composers']):
            composer_index = data_prefix['composers'][index]['person']
            if str(composer_index) not in person_links:
                person_links[f'{composer_index}'] = fetch_meta_data(composer_index, 'person')
                save_meta = True
            composers.append({"@type": "schema:Person", "@id": copy.deepcopy(person_links[f'{composer_index}'])})
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


def enrich_performers(data_prefix, role):
    global save_meta
    performers = []
    for index, person in enumerate(data_prefix['persons']):
        person_index = data_prefix['persons'][index]['person']
        if str(person_index) not in person_links:
            person_links[f'{person_index}'] = fetch_meta_data(person_index, 'person')
            save_meta = True
        if role:
            occupation_index = data_prefix['persons'][index]['subject']
            if str(occupation_index) not in subject_links:
                subject_links[f'{occupation_index}'] = fetch_meta_data(occupation_index, 'subject')
                save_meta = True
            performers.append({'@type': 'schema:Person', '@id': copy.deepcopy(person_links[f'{person_index}']),
                               'schema:hasOccupation': copy.deepcopy(subject_links[f'{occupation_index}'])})
        else:
            performers.append({'@type': 'schema:Person', '@id': copy.deepcopy(person_links[f'{person_index}'])})
    for index, corporation in enumerate(data_prefix['corporations']):
        corporation_index = data_prefix['corporations'][index]['corporation']
        if str(corporation_index) not in corporation_links:
            corporation_links[f'{corporation_index}'] = fetch_meta_data(corporation_index, 'corporation')
            save_meta = True
        if role:
            occupation_index = data_prefix['corporations'][index]['subject']
            if str(occupation_index) not in subject_links:
                subject_links[f'{occupation_index}'] = fetch_meta_data(occupation_index, 'subject')
                save_meta = True
            performers.append(
                {'@type': 'schema:PerformingGroup', '@id': copy.deepcopy(corporation_links[f'{corporation_index}']),
                 'schema:description': copy.deepcopy(subject_links[f'{occupation_index}'])})
        else:
            performers.append({'@type': 'schema:PerformingGroup', '@id': copy.deepcopy(corporation_links[f'{corporation_index}'])})
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

    work_template = load_template('work')
    works = harvest_category(10, 'work')
    mapped_work = []
    for index, work in enumerate(works):
        work = map_json_data(work, work_template, index)
        mapped_work.append(copy.deepcopy(work))
    save_json_category_data(mapped_work, f'work_feed/')

    if save_meta:
        save_meta_data_to_json(location_links, 'meta/location.json')
        save_meta_data_to_json(series_links, 'meta/series.json')
        save_meta_data_to_json(source_links, 'meta/sources.json')
        save_meta_data_to_json(person_links, 'meta/persons.json')
        save_meta_data_to_json(subject_links, 'meta/subjects.json')
        save_meta_data_to_json(corporation_links, 'meta/corporations.json')
        save_meta_data_to_json(work_links, 'meta/works.json')
        save_meta_data_to_json(event_auth, 'meta/events.json')


def fetch_meta_data(index, category):
    authority_linked = {}
    data = fetch_json_data(f"https://performance.musiconn.de/api?action=get&format=json&{category}={str(index)}")
    link = format_link(data, category, index)
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


def format_link(meta_data, category, index):
    link = meta_data[category][str(index)]['url']
    return link


def save_meta_data_to_json(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(f"{file_path}", 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)
        print(f"\nData saved to {file_path}")


def load_meta_data():
    global location_links
    global series_links
    global source_links
    global person_links
    global subject_links
    global corporation_links
    global work_links
    global event_auth

    if os.path.exists('meta/location.json'):
        with open('meta/location.json', 'r', encoding='utf-8') as file:
            location_links = json.load(file)

    if os.path.exists('meta/series.json'):
        with open('meta/series.json', 'r', encoding='utf-8') as file:
            series_links = json.load(file)

    if os.path.exists('meta/sources.json'):
        with open('meta/sources.json', 'r', encoding='utf-8') as file:
            source_links = json.load(file)

    if os.path.exists('meta/persons.json'):
        with open('meta/persons.json', 'r', encoding='utf-8') as file:
            person_links = json.load(file)

    if os.path.exists('meta/subjects.json'):
        with open('meta/subjects.json', 'r', encoding='utf-8') as file:
            subject_links = json.load(file)

    if os.path.exists('meta/corporations.json'):
        with open('meta/corporations.json', 'r', encoding='utf-8') as file:
            corporation_links = json.load(file)

    if os.path.exists('meta/works.json'):
        with open('meta/works.json', 'r', encoding='utf-8') as file:
            work_links = json.load(file)

    if os.path.exists('meta/events.json'):
        with open('meta/events.json', 'r', encoding='utf-8') as file:
            event_auth = json.load(file)


if __name__ == "__main__":
    load_meta_data()
    process_json_data()

