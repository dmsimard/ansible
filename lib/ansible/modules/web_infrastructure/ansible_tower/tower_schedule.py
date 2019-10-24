#!/usr/bin/python
# coding: utf-8 -*-

# (c) 2018, Red Hat
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type


ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: tower_schedule
author:
    - Blair Morrison
    - David Moreau-Simard (@dmsimard)
version_added: 2.7
short_description: Create, update, or destroy Ansible Tower schedules
description:
    - Create, update, or destroy Ansible Tower schedules. See
      U(https://www.ansible.com/tower) for an overview.
options:
    name:
        description:
            - The name to use for the schedule.
        required: true
    state:
        description:
            - Desired state of the schedule.
        choices: ["present", "absent", "disabled"]
        default: "present"
    description:
        description:
            - The description to use for the schedule.
        required: false
    job_template:
        description:
            - The name of the job template this schedule applies to.
        required: false
        choices: ["run", "check"]
    job_type:
        description:
            - The type of the job template this schedule applies to.
        required: false
    project:
        description:
            - The name of the project this schedule applies to.
        required: false
    inventory_source:
            - The name of the inventory source this schedule applies to.
        required: false
    start:
        description:
            - The date and time when the schedule should be effective.
              This must be in the ISO 8601 format and in the UTC timezone,
              e.g. '2018-01-01T00:00:00Z'.
              The module will fail if dates are provided in any other format.
        required: false
        default: immediately, equivalent to the ansible_date_time.iso8601 fact
    frequency:
        description:
            - The number of frequency_unit intervals between each execution of the specified resource
              (job template, project or inventory source)
        type: int
        required: false
    frequency_unit:
        description:
            - The duration of each time interval.
        choices: ["runonce", "minute", "hour", "day"]
        required: false
        default: runonce
    extra_data:
        description:
            - Extra variables passed to the schedule.
        required: false
        type: dict
    limit:
        description:
            - A host pattern to further constrain the list of hosts managed or affected by the schedule
extends_documentation_fragment: tower
'''

EXAMPLES = '''
- name: Create a schedule to update a project every 30 minutes
  tower_schedule:
    state: present
    project: git project
    frequency: 30
    frequency_unit: minute
- name: Disable the schedule for an inventory source
  tower_schedule:
    state: disabled
    inventory_source: cloud
    frequency: 30
    frequency_unit: minute
- name: Create a schedule to run a job template once at a specific time
  tower_schedule:
    state: present
    job_template: upgrade something
    start: 2018-01-01T00:00:00Z
    frequency_unit: runonce
'''

from datetime import datetime

from ansible.module_utils.ansible_tower import TowerModule, tower_auth_config, tower_check_mode

try:
    import yaml
    import tower_cli
    import tower_cli.exceptions as exc

    from tower_cli.conf import settings
except ImportError:
    pass

def parse_datetime_string(module, dt_string):
    """
    This accepts a datetime string in ISO 8601 format in UTC time and returns
    a string representation of that datetime that can be used to construct the rrule
    string that is used by Ansible Tower.
    """
    try:
        dt = datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%SZ")
        result = datetime.strftime(dt, "%Y%m%dT%H%M%SZ")
    except ValueError as excinfo:
        module.fail_json(
            msg="Failed to update schedule, unable to parse datetime {0}: {1}".format(dt_string, excinfo),
            changed=False
        )
    return result

def build_rrule(startdate, freq, freq_unit):
    """
    Generate the RRULE string.
    Currently only supports the units: MINUTELY, HOURLY, DAILY, and RUNONCE
    """
    unit_map = {
                'minute': 'MINUTELY',
                'day': 'DAILY',
                'hour': 'HOURLY',
                'runonce': 'DAILY'
    }
    result = 'DTSTART:{0} RRULE:FREQ={1};INTERVAL={2}'.format(startdate, unit_map[freq_unit], freq)
    if freq_unit == 'runonce':
        result += 'COUNT=1'

    return result

def update_fields(p):
    '''This updates the module field names
    to match the field names tower-cli expects to make
    calling of the modify/delete methods easier.
    '''
    params = p.copy()
    field_map = {
    }

    params_update = {}
    for old_k, new_k in field_map.items():
        v = params.pop(old_k)
        params_update[new_k] = v

    extra_data = params.get('extra_data')
    if extra_data is not None:
        params_update['extra_data'] = yaml.dump(extra_data)

    params.update(params_update)
    return params

def update_resources(module, p):
    params = p.copy()

    identity_map = {
        'job_template': 'name',
        'project': 'name',
        'inventory_source': 'name',
    }
    for k, v in identity_map.items():
        try:
            if params[k]:
                result = tower_cli.get_resource(k).get(**{v: params[k]})
                params[k] = result['id']
            elif k in params:
                # unset empty parameters to avoid ValueError: invalid literal for int() with base 10: ''
                del(params[k])
        except (exc.NotFound) as excinfo:
            module.fail_json(msg='Failed to update schedule: {0}'.format(excinfo), changed=False)
    return params

def main():
    argument_spec=dict(
        name=dict(required=True),
        state=dict(choices=['present', 'absent', 'disabled'], default='present'),
        description=dict(default=''),
        job_template=dict(),
        job_type=dict(choices=['run', 'check']),
        project=dict(),
        inventory_source=dict(),
        start=dict(default=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")),
        frequency=dict(type='int'),
        frequency_unit=dict(choices=['runonce', 'minute', 'hour', 'day'], default='runonce'),
        extra_data=dict(type='dict', required=False),
        limit=dict(default=''),
    )
    mutually_exclusive = ['job_template', 'project', 'inventory_source']
    module = TowerModule(argument_spec=argument_spec, mutually_exclusive=mutually_exclusive, supports_check_mode=True)

    name = module.params.get('name')
    state = module.params.pop('state')
    frequency = module.params.pop('frequency')
    frequency_unit = module.params.pop('frequency_unit')
    start =  module.params.pop('start')
    json_output = {'schedule': name, 'state': state}

    tower_auth = tower_auth_config(module)
    with settings.runtime_values(**tower_auth):
        tower_check_mode(module)
        schedule = tower_cli.get_resource('schedule')

        params = update_resources(module, module.params)
        params = update_fields(params)
        params['create_on_missing'] = True
        params['enabled'] = not state == 'disabled'

        dtstart = parse_datetime_string(module, start)
        params['rrule'] = build_rrule(dtstart, frequency, frequency_unit)

        try:
            if state == 'absent':
                result = schedule.delete(**params)
            else:
                result = schedule.modify(**params)
                json_output['id'] = result['id']
        except (exc.ConnectionError, exc.BadRequest, exc.NotFound) as excinfo:
            module.fail_json(msg='Failed to update schedule: {0}'.format(excinfo), changed=False)

        json_output['changed'] = result['changed']
        module.exit_json(**json_output)


from ansible.module_utils.basic import AnsibleModule

if __name__ == '__main__':
    main()
