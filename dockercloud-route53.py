#!/usr/bin/env python3

# vim modeline (put ":set modeline" into your ~/.vimrc)
# vim:set et ts=4 sw=4 ai ft=python:

"""
Docker Cloud Agent

Created by Brandon Gillespie

Watches services in Docker Cloud, and places them into Route53 Domain

Configuration comes in via $CONFIG env var, which is a base64 json object.

If $CONFIG is missing, it looks to STDIN (using Reflex Engine)

See config-default.json for a sample.
"""

import re
import sys
import base64
import os
import select # forever wait
import threading
import traceback
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import ujson as json
import onetimejwt
import time
import rfx
import dictlib
import boto3
import dictlib
import logging

boto3.set_stream_logger('boto3.resources', logging.DEBUG)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

###############################################################################
# pylint: disable=too-many-instance-attributes
class DockerCloud(rfx.Base):
    """Docker Cloud Agent Object"""

    aws = None
    dc = None
    stats = None

    ###########################################################################
    # pylint: disable=super-init-not-called
    def __init__(self, base=None, debug=None):
        super(DockerCloud, self).__init__(debug=debug)
        if base:
            rfx.Base.__inherit__(self, base)
        self.aws = dictlib.Obj(r53c=None)
        self.dc = dictlib.Obj()
        self.lock = threading.Lock()
        self.timestamp = True

    ###########################################################################
    def reset_stats(self):
        self.stats = dictlib.Obj(services=0, changes=0, started=time.time())

    ###########################################################################
    def start_agent(self):
        """Run as a service"""

        try:
            raw_conf = os.environ.get('CONFIG', None)
            if not raw_conf:
                raw_conf = json.load(stdin)
            if raw_conf and raw_conf[0] != "{":
                raw_conf = base64.b64decode(raw_conf).decode()
            cfg = json.loads(raw_conf)

        except Exception as err: # pylint: disable=broad-except
            self.ABORT("Unable to find config at $CONFIG or stdin... not run correctly?\n" +
                       "Error: " + str(err))

        cfg = dictlib.dig(cfg, 'sensitive.config')

        if dictlib.dig(cfg, 'debug'):
            self.debug = {'*': True}

        # todo: remove this (and figure out why it isn't sticking from cmdline)
        self.service = os.environ.get("APP_SERVICE", 'unknown')

        # pull in configs from stdin
        poll_interval = dictlib.dig(cfg, 'dockercloud.poll_interval_seconds') * 1000

        dc_user = dictlib.dig(cfg, 'dockercloud.user')
        dc_pass = dictlib.dig(cfg, 'dockercloud.pass')
        self.dc['scope'] = dc_user
        self.dc.cfg = dictlib.Obj(dictlib.dig(cfg, "dockercloud"))
        self.dc.headers = {
            "Authorization":"Basic " + base64.b64encode((dc_user + ":" + dc_pass).encode()).decode(),
            "Accept":"application/json"
        }

        self.aws = dictlib.Obj(cfg['aws'])
        self.aws['r53c'] = None

        self.NOTIFY("Starting Docker Cloud => route53 agent", account=dc_user,
                    interval=int(poll_interval/1000))

        # this is the heart of the program, it runs on an interval
        self.update()
        interval_stopper = rfx.set_interval(poll_interval, self.update)

        # wait forever
        try:
            select.select([], [], [])
        except KeyboardInterrupt:
            interval_stopper.set()

    ###########################################################################
    def r53_init(self):
        if not self.aws.r53c:
            self.DEBUG("Route53 Connect")
            self.aws.r53c = boto3.client('route53', **self.aws.connect)

    ###########################################################################
    def r53_update(self, name, rtype, values, add=True, delete=True, type="UPSERT"):
        self.r53_init()

        aws_changes = list()
        zone_add = list()
        zone_del = list()
        name = name.lower()
        rtype = rtype.upper()
        zone = self.aws.r53zone
        starting = set(zone.get(name,{}).get(rtype,{}).keys())

        if add:
            for value in values:
                value = value.lower()
                starting.discard(value)
                if zone.get(name,{}).get(rtype,{}).get(value):
                    continue

                self._r53_do({
                    'Action': type,
                        'ResourceRecordSet': {
                            'Name': name + '.' + self.aws.r53.domain,
                            'Type': rtype.upper(),
                            'TTL': self.aws.r53.ttl,
                            'ResourceRecords': [{
                               'Value': value
                            }]
                        }
                    })
                self._r53_do({
                    'Action': 'UPSERT',
                        'ResourceRecordSet': {
                            'Name': name + '.' + self.aws.r53.domain,
                            'Type': 'TXT',
                            'TTL': self.aws.r53.ttl,
                            'ResourceRecords': [{
                               'Value': '"v=dca"'
                            }]
                        }
                    })

        if delete:
            for value in starting:
                record = zone[name][rtype][value]
                self._r53_do({
                    'Action': 'DELETE',
                        'ResourceRecordSet': {
                            'Name': name + '.' + self.aws.r53.domain,
                            'Type': rtype,
                            'TTL': record.get('TTL', self.aws.r53.ttl),
                            'ResourceRecords': [{
                               'Value': value
                            }]
                        }
                    })
                self._r53_do({
                    'Action': 'DELETE',
                        'ResourceRecordSet': {
                            'Name': name + '.' + self.aws.r53.domain,
                            'Type': 'TXT',
                            'TTL': self.aws.r53.ttl,
                            'ResourceRecords': [{
                               'Value': '"v=dca"'
                            }]
                        }
                    })

        return True

    def _r53_do(self, change):

        try:
            values = list()
            for val in change['ResourceRecordSet']['ResourceRecords']:
                values.append(val['Value'])
            self.NOTIFY("Route53 {} {} {} {}"
                        .format(change['Action'],
                                change['ResourceRecordSet']['Name'],
                                change['ResourceRecordSet']['Type'],
                                " ".join(values)
                                ))
            self.stats.changes += 1
            return self.aws.r53c.change_resource_record_sets(
                HostedZoneId= self.aws.r53.zone_id,
                ChangeBatch= {'Changes': [change]}
            )
        except:
            self.NOTIFY("Error talking to Route53! " + traceback.format_exc())

    ###########################################################################
    def _r53_zone_add(self, name, rtype, value, record):
        name = name.lower()
        rtype = rtype.upper()
        value = value.lower()

        zone = self.aws.r53zone

        if not zone.get(name):
            zone[name] = { rtype : dict() }

        elif not zone[name].get(rtype):
            zone[name][rtype] = dict()

        zone[name][rtype][value] = record

    ###########################################################################
    def r53_load_zone(self):
        """Go to AWS and get all dns records"""

        self.DEBUG("Route53 Load zoneid=" + self.aws.r53.zone_id + " domain=" + self.aws.r53.domain)
        self.r53_init()
        self.aws['r53_dca'] = set()

        # goofy boto pagination and iterator gets us away from having to do it ourself
        r53_paginator = self.aws.r53c.get_paginator('list_resource_record_sets')
        r53_iterator = r53_paginator.paginate(
            HostedZoneId=self.aws.r53.zone_id,
        )
        self.aws['r53zone'] = zone = dict()
        strip_name_rx = re.compile("\\." + self.aws.r53.domain.replace(".", "\\.") + "$")
        count = 0
        for record_set in r53_iterator:
            for record in record_set['ResourceRecordSets']:
                count += 1
                rname = record['Name']
                short_name = strip_name_rx.sub('', rname)
                record['ShortName'] = short_name
                rtype = record['Type']

                for vdict in record.get('ResourceRecords', []):
                    if vdict.get('Value'):
                        value = vdict['Value']
                        if rtype == 'TXT':
                            if value[:6] == '"v=dca':
                                self.aws.r53_dca.add(short_name)

                        self._r53_zone_add(short_name, record['Type'], value, record)

        self.DEBUG("Route53 Loaded {} records, filtered to {}".format(count, len(zone)))

        return zone

    ###########################################################################
    def dc_load_services(self):
        """Query DockerCloud and identify cluster groups"""

        self.DEBUG("Docker Cloud Load Services...")

        # short cut
        def from_set(dictionary, key1, key2):
            """Helper to simplify code"""
            try:
                return dictionary.get(key1, [{}])[0].get(key2, "")
            except: # pylint: disable=bare-except
                return None

        services = dict()

        for dc_state in ["Running"]:
            clist = self.dc_api(resource="container", query="/?state=" + dc_state)
            for cnt in clist.get('objects', []):
                name = cnt.get('name')
                try:
                    node = self.dc_api(query="/" + cnt.get('node', {}), full=True, cached=True)
                    port = from_set(cnt, "container_ports", "outer_port")
                    pip = from_set(node, "private_ips", "cidr")
                    if pip:
                        pip = pip.split("/")[0]
                    cur = services.get(name, [])
                    cur.append((name, pip, port))
                    services[name] = cur
                except Exception as err: # pylint: disable=broad-except
                    self.message_team("Unable to process container: {} ({})".format(name, err))
                    self.NOTIFY("Traceback", traceback=traceback.format_exc())
    
        return services

    ###########################################################################
    def dc_api(self, system=None, resource=None, query=None, full=False, cached=False):
        """Call Docker Cloud API"""

        # Todo: TRY/CATCH for exceptions and wait
        base = "https://cloud.docker.com"
        if full:
            if query[0:2] == "//":
                query = query[1:]
            url = base + query
        else:
            if not system:
                system = "app"

            res = self.dc.cfg.url.get(resource)
            if not res:
                raise ValueError("Cannot find config.dockercloud.url." + resource)

            url = base + "/api/" + system + "/" + res.vers
            if self.dc.cfg.org:
                url += "/" + self.dc.cfg.org
            url += res.path + query

        if cached and url in self.cache:
            return self.cache[url]

        res = requests.get(url, headers=self.dc.headers)

        if res.status_code != 200:
            # alarm somehow
            msg = "Unable to query Docker Cloud! {}".format(res.status_code)
            self.message_team(msg)
            raise ValueError(msg)

        obj = res.json()
        if cached:
            self.cache[url] = obj

        return obj

    ###########################################################################
    def message_team(self, msg):
        """Send a notification to appropriate channel"""
        return

        msg = "> " + self.service + " Docker Cloud: " + msg
        self.slack.send(self.slack_channel, msg)

    ###########################################################################
    @rfx.threadlock
    def update(self):
        """Run on interval, query docker cloud and update things"""

        self.DEBUG("Update Starting")
        self.reset_stats()

        # clear our cache
        self.cache = dict()

        # what we care about from DSE
        zone = self.r53_load_zone()

        # what is in docker cloud right now
        services = self.dc_load_services()

        clusters = dict()

        bad_name_rx = re.compile('[^a-z0-9-]')
        cluster_rx = re.compile('-[0-9]+$')

        dc_names1 = self.aws.r53_dca # zone.get('docker-cloud-managed-names', {}).get('TXT', {}).keys())

        # iterate what is in docker cloud
        dc_names2 = set()
        domain = self.aws.r53.domain
        for name in services:
            self.stats.services += 1
            if bad_name_rx.search(name):
                self.DEBUG("ignoring bad service name: " + name)

            dc_names2.add(name)
            ips = set()
            # a list of tuples with name:ip:port
            for addr in services[name]:
                ips.add(addr[1])

            cluster = cluster_rx.sub('', name)
            dc_names2.add(cluster)
            if not clusters.get(cluster):
                clusters[cluster] = set(ips)
            else:
                for ip in ips:
                    clusters[cluster].add(ip)

            self.r53_update(name, "A", list(ips))

        for cluster in clusters:
            # should pull the current state of cluster and get a set difference
            vals = list()
            x=0
            for ip in sorted(clusters[cluster]):
                vals.append({'Value': ip})
                # we also want to create a normalized numbered name,
                # because docker cloud picks random numbers...
                x += 1
                nname = "{}-{}n".format(cluster, x)
                dc_names2.add(nname)

                self.r53_update(nname, "A", [ip])

            current = set(zone.get(cluster,{}).get("A",{}).keys())
            if not current.difference(clusters[cluster]):
                continue
            self._r53_do({
                'Action': "UPSERT",
                    'ResourceRecordSet': {
                        'Name': cluster + '.' + self.aws.r53.domain,
                        'Type': "A",
                        'TTL': self.aws.r53.ttl,
                        'ResourceRecords': vals
                    }
                })

        for name in dc_names1.difference(dc_names2):
            self.r53_update(name, "A", [], add=False) # delete only

        self.stats['duration'] = time.time() - self.stats.started
        self.DEBUG("Update finished, services={services} changes={changes} duration=\"{duration:0.2f} seconds\"".format(**self.stats))

def main():
    cm = DockerCloud()
    cm.start_agent()

if __name__ == "__main__":
    main()

