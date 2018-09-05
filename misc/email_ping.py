# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 13:55:43 2018

@author: cagurl01
"""

# Modified from free script posted by Scott Brady, gh: scottbrady91

import re
import smtplib
import dns.resolver


# Address used for SMTP MAIL FROM command
from_address = input('Please enter the from email address: ')

# Simple Regex for syntax checking
regex = r'^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,})$'

# Email address to verify
input_address = input('Please enter the email address to verify: ')
address_to_verify = str(input_address)

# Syntax check
match = re.match(regex, address_to_verify)
if match is None:
    print('Bad Syntax')
    raise ValueError('Bad Syntax')

# Get domain for DNS lookup
split_address = address_to_verify.split('@')
domain = str(split_address[1])
print('Domain:', domain)

# MX record lookup
records = dns.resolver.query(domain, 'MX')
mx_record = records[0].exchange
mx_record = str(mx_record)
print(mx_record)


# SMTP lib setup (use debug level for full output)
#server = smtplib.SMTP()
code = None
message = None
with smtplib.SMTP() as server:
    server.set_debuglevel(0)

    # SMTP Conversation
    server.connect(mx_record)
    server.helo(server.local_hostname)  # server.local_hostname(Get local server hostname)
    print(server)
    print(server.local_hostname)
    server.mail(from_address)
    code, message = server.rcpt(str(address_to_verify))
#server.quit()

print(code)
print(message)

# Assume SMTP response 250 is success
if code == 250:
    print('Success')
else:
    print('Bad')
