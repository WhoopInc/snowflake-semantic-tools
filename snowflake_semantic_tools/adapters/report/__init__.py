"""HTML reports SST writes beside the JSON a run produced: self-contained, read in any browser.

A report is one document with its styles inline and its charts as inline SVG. It loads nothing:
no script, no stylesheet, font, or image from anywhere, so it renders the same offline, attached
to a ticket, or printed. Every value read from the JSON is escaped on its way in.
"""
