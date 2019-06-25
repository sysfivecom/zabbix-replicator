#!/bin/python
# -*- coding: utf-8 -*-
"""
Replicating Data from one Zabbix server to another

This script should run as a cronjob every minute or however you like.
It uses flock to make sure that only one instance is running at a time,
so it still works fine when it sometimes need a long time to replicate after
adding hosts or connection outages.

It replicates item+trigger configs and the item data from one zabbix
server (the primary) to another one (the replica).

It requests the config/data via the Zabbix API (so needs a user/login
who has the necessary read rights etc) and writes the config also
via API (needing a write user on that one), while using the Zabbix
Sender Protocol for the data replication into the replica

The hostnames to be replicated must be configured in a json config file,
it then tries to replicate all items and triggers of it. Configuration
replication is done if it was not possible to send all data successfully
(thus assuming missing items on the replica as the cause).

It adds a replication.timestamp item to keep track of when the replication
was done successfully the last time. This script also provides a trigger
for this item and makes all replicated triggers dependent on this.

All replicated items are configured as "Zabbix Trapper Items" in the replica
to make it possible to use the Zabbix Sender Protocol for them.
"""
import argparse
import fcntl
import json
import pprint
import os
import sys
import time
from pyzabbix import ZabbixAPI
from pyzabbix import ZabbixMetric
from pyzabbix import ZabbixSender

config_file = "/vagrant/replica/example-config.json"


def get_timestamp(zbx, hostname, config):
    """ This method should get the timestamp of the last successful
    replication of the given host. It must be checked if it exists already,
    because if this is the first time a host shall be replicated, the host
    is not yet created. In that case, the timestamp is used as defined by
    the initial_time variable to start with data from a certain period """
    now_time = int(time.time())
    item = zbx.item.get(filter={
                            "host": hostname,
                            "name": config["timestamp_item"]["name"]})
    if len(item) == 0:
        # item is not yet existing, needs to be created and last time is 0
        create_or_update_item(zbx, hostname, config["timestamp_item"], config)
        # we only want to replace hostname in the copy
        trigger = config["timestamp_trigger"].copy()
        trigger["expression"] = trigger["expression"].replace("HOSTNAME",
                                                              hostname)
        create_or_update_trigger(zbx, hostname, trigger)
        last_time = now_time - config["initial_history"]
    else:
        itemid = item[0]['itemid']
        history = zbx.history.get(itemids=itemid,
                                  sortorder="DESC",
                                  sortfield='clock',
                                  limit=1)
        if len(history) == 0:
            # first replication - item exists, but did never receive any data
            last_time = now_time - config["initial_history"]
        else:
            last_time = history[0]['value']
    return last_time, now_time


def send_timestamp(sender, hostname, itemname, timestamp):
    """ When the data was completely replicated, the timestamp of
    the replication is send to the Zabbix replica """
    metrics = []
    m = ZabbixMetric(hostname, itemname, timestamp)
    metrics.append(m)
    sender.send(metrics)


def replicate_hostdata(primary,
                       replica,
                       sender,
                       hostname,
                       last_time,
                       now_time,
                       config):
    """ This method replicates all data for one host in a given timeframe.
    If it should not be possible to send all items, this method calls the
    config replication to make it possible the next time """
    print "INFO: Replicating Data for " + hostname
    pp = pprint.PrettyPrinter(indent=4)
    try:
        with open(os.path.join(config["map_dir"],
                  hostname + ".json"),
                  "r") as f:
            item_map = json.loads(f.read())
    except:
        # when no config exists for that host, create it new
        item_map = replicate_hostconfig(primary, replica, hostname, config)
    float_values = [key for key, val in item_map.items()
                    if val["valtype"] == "0"]
    char_values = [key for key, val in item_map.items()
                   if val["valtype"] == "1"]
    unsigned_values = [key for key, val in item_map.items()
                       if val["valtype"] == "3"]
    text_values = [key for key, val in item_map.items()
                   if val["valtype"] == "4"]
    metrics = []
    if len(float_values) > 0:
        item_history = primary.history.get(itemids=float_values,
                                           time_from=last_time,
                                           time_till=now_time,
                                           history="0")
        for metric in item_history:
            m = ZabbixMetric(hostname,
                             item_map[metric["itemid"]]['key'],
                             metric['value'],
                             int(metric['clock']))
            metrics.append(m)
    if len(char_values) > 0:
        item_history = primary.history.get(itemids=char_values,
                                           time_from=last_time,
                                           time_till=now_time,
                                           history="1")
        for metric in item_history:
            m = ZabbixMetric(hostname,
                             item_map[metric["itemid"]]['key'],
                             metric['value'],
                             int(metric['clock']))
            metrics.append(m)
    if len(unsigned_values) > 0:
        item_history = primary.history.get(itemids=unsigned_values,
                                           time_from=last_time,
                                           time_till=now_time,
                                           history="3")
        for metric in item_history:
            m = ZabbixMetric(hostname,
                             item_map[metric["itemid"]]['key'],
                             metric['value'],
                             int(metric['clock']))
            metrics.append(m)
    if len(text_values) > 0:
        item_history = primary.history.get(itemids=text_values,
                                           time_from=last_time,
                                           time_till=now_time,
                                           history="4")
        for metric in item_history:
            m = ZabbixMetric(hostname,
                             item_map[metric["itemid"]]['key'],
                             metric['value'],
                             int(metric['clock']))
            metrics.append(m)
    response = sender.send(metrics)
    pp.pprint(response)
    if response.failed > 0:
        print "WARN: Replication of Data not successful"
        # sending of items failed
        # try to update the config and try to send again
        item_map = replicate_hostconfig(primary, replica, hostname, config)
    else:
        send_timestamp(sender,
                       hostname,
                       config["timestamp_item"]["name"],
                       now_time)
        print "INFO: Replication of Data successful"


def replicate_hostconfig(primary, replica, hostname, config):
    """ This method replicates item and trigger configs for the
    given hostname from primary to replica server.
    It converts all items to "Zabbix Trapper" type and additionally
    adds a dependency to the replication trigger to all triggers"""
    print "INFO: Replicating Config for " + hostname
    items_raw = primary.item.get(filter={"host": hostname})
    items = []
    item_map = {}
    for raw in items_raw:
        clean = {
                "key_": raw["key_"],
                "name": raw["name"],
                "type": 2,
                "value_type": raw["value_type"],
                "description": raw["description"],
                "history": raw["history"],
                "status": raw["status"],
                "trends": raw["trends"]
                }
        items.append(clean)
        item_map[raw["itemid"]] = {"key": raw["key_"],
                                   "valtype": raw["value_type"]}
    for item in items:
        create_or_update_item(replica, hostname, item, config)
    # replicate trigger configs
    triggers_raw = primary.trigger.get(filter={"host": hostname},
                                       expandExpression=True,
                                       sortfield="triggerid",
                                       selectDependencies="extend")
    triggers = []
    for raw in triggers_raw:
        clean = {
                "comments": raw["comments"],
                "description": raw["description"],
                "expression": raw["expression"],
                "priority": raw["priority"],
                "status": raw["status"],
                "type": raw["type"],
                "recovery_mode": raw["recovery_mode"],
                "recovery_expression": raw["recovery_expression"],
                "correlation_mode": raw["correlation_mode"],
                "manual_close": raw["manual_close"]
                }
        clean["dep_names"] = [config["timestamp_trigger"]["description"]]
        for dep in raw["dependencies"]:
            clean["dep_names"].append(dep["description"])
        triggers.append(clean)
    for trigger in triggers:
        r = create_or_update_trigger(replica, hostname, trigger)
    with open(os.path.join(config["map_dir"], hostname + ".json"), "w") as f:
        f.write(json.dumps(item_map))
    return item_map


def create_host(replica, hostname, config):
    """ This method simply creates a new host on the replica server """
    host = {
            "host": hostname,
            "groups": config["groups"],
            "interfaces": config["interfaces"]
            }
    replica.host.create(host)


def create_or_update_item(replica, hostname, item, config):
    """ This method checks if an item already exists on the replica
    and either creates or updates it as it is defined on the primary """
    hostid = replica.get_id("host", hostname)
    if hostid is None:
        create_host(replica, hostname, config)
        hostid = replica.get_id("host", hostname)
    item["hostid"] = hostid
    items_raw = replica.item.get(filter={"host": hostname,
                                         "key_": item["key_"]})
    if len(items_raw) == 0:
        itemid = None
    else:
        itemid = items_raw[0]["itemid"]
    pp = pprint.PrettyPrinter(indent=4)
    if itemid is None:
        print "Creating: " + item["key_"]
        result = replica.item.create(item)
    else:
        print "Updating: " + item["key_"]
        item["itemid"] = itemid
        result = replica.item.update(item)


def create_or_update_trigger(replica, hostname, trigger):
    """ This method checks if a trigger already exists on the replica
    and either creates or updates it as it is defined on the primary """
    result = replica.trigger.get(
            filter={"host": hostname,
                    "description": trigger["description"]})
    dep_ids = []
    dependencies = trigger.pop("dep_names", [])
    for dep in dependencies:
        r = replica.trigger.get(filter={"host": hostname, "description": dep})
        if len(r) == 1:
            dep_ids.append({"triggerid": r[0]["triggerid"]})
    trigger["dependencies"] = dep_ids
    pp = pprint.PrettyPrinter(indent=4)
    if len(result) == 1:
        trigger["triggerid"] = result[0]["triggerid"]
        print "Updating: " + trigger["description"]
        pp.pprint(replica.trigger.update(trigger))
    else:
        print "Creating: " + trigger["description"]
        pp.pprint(replica.trigger.create(trigger))


def zabbix_login(zabbixurl, user, password):
    """ This method is reponsible for the login to zabbix """
    zbx = ZabbixAPI(zabbixurl, user=user, password=password)
    return zbx


def main():
    # parse parameters
    # create zabbix communication objects
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="replica configuration file")
    parser.add_argument("-r",
                        "--replicate-config",
                        help=("Force a replication of the full item/trigger"
                              "configuration from one server to another"),
                        action="store_true")
    args = parser.parse_args()
    # load config from file
    with open(args.config_file, "r") as f:
        config = json.loads(f.read())
    # make sure only one instance is running per config file
    fh = open(os.path.realpath(__file__), 'r')
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except:
        print "Already one instance running"
        os._exit(0)
    # get important values from config
    primary = zabbix_login(config["primary"]["url"],
                           config["primary"]["user"],
                           config["primary"]["password"])
    replica = zabbix_login(config["replica"]["url"],
                           config["replica"]["user"],
                           config["replica"]["password"])
    sender = ZabbixSender(config["replica"]["ip"],
                          int(config["replica"]["port"]))

    pp = pprint.PrettyPrinter(indent=4)
    # explicitly perform config replication
    if args.replicate_config:
        for hostname in config["replication_hosts"]:
            item_map = replicate_hostconfig(primary, replica, hostname, config)
            pp.pprint(item_map)

    # perform data replication and if necessary config replication
    for hostname in config["replication_hosts"]:
        last_time, now_time = get_timestamp(replica,
                                            hostname,
                                            config)
        replicate_hostdata(primary,
                           replica,
                           sender,
                           hostname,
                           last_time,
                           now_time,
                           config)


if __name__ == "__main__":
    main()
