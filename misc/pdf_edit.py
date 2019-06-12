# -*- coding: utf-8 -*-
"""
Created on Wed Jun 12 12:38:49 2019

@author: cagurl01
"""

import PyPDF2 as pdf


#reader = pdf.PdfFileReader('test.pdf')
#page = reader.getPage(0)
#page.rotateClockwise(90)
#page = reader.getPage(1)
#page.rotateClockwise(90)
#reader.stream.close()

counter = 0
reader = pdf.PdfFileReader('test.pdf')
writer = pdf.PdfFileWriter()
while counter < reader.numPages:
    page = reader.getPage(counter)
    if counter < 2:
        page.rotateClockwise(90)
    writer.addPage(page)
    counter += 1
with open('test.pdf', 'wb') as file:
    writer.write(file)
reader.stream.close()
