import unittest
import pytz

from os import path
from datetime import datetime
from unittest.mock import MagicMock

from src import amwager_parser as amwparser

RES_PATH = './tests/resources'


class TestPostTime(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.local_zone = amwparser.get_localzone
        amwparser.get_localzone = MagicMock()
        amwparser.get_localzone.return_value = pytz.UTC
        return

    def tearDown(self):
        super().tearDown()
        amwparser.get_localzone = self.local_zone
        return

    def test_empty_html(self):
        post = amwparser.get_post_time('')
        self.assertEqual(post, None)

    def test_html_is_none(self):
        post = amwparser.get_post_time(None)
        self.assertEqual(post, None)

    def test_no_post_time_listed(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            post = amwparser.get_post_time(html.read())
            self.assertEqual(post, None)
            amwparser.get_localzone.assert_not_called()

    def test_correct_post_time(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            post = amwparser.get_post_time(html.read())
            expected = datetime(1900, 1, 1, 16, 15, 0, tzinfo=pytz.UTC)
            self.assertEqual(post, expected)
            amwparser.get_localzone.assert_called_once()
            amwparser.get_localzone.reset_mock()

    def test_localization(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            amwparser.get_localzone.return_value = pytz.timezone('CET')
            post = amwparser.get_post_time(html.read())
            expected = datetime(1900, 1, 1, 15, 15, 0, tzinfo=pytz.UTC)
            self.assertEqual(post, expected)
            amwparser.get_localzone.assert_called_once()


class TestMTP(unittest.TestCase):
    def test_mtp_listed(self):
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            mtp = amwparser.get_mtp(html.read())
            self.assertEqual(mtp, 5)

    def test_mtp_not_listed(self):
        file_path = path.join(RES_PATH, 'amw_post_time.html')
        with open(file_path, 'r') as html:
            mtp = amwparser.get_mtp(html.read())
            self.assertEqual(mtp, None)

    def test_empty_html(self):
        post = amwparser.get_post_time('')
        self.assertEqual(post, None)

    def test_html_is_none(self):
        post = amwparser.get_post_time(None)
        self.assertEqual(post, None)


class TestTrackListParsing(unittest.TestCase):
    def test_track_list(self):
        expected = [{
            'id':
            'TDM',
            'html':
            '<a class="event_selector event-status-C" id="TDM" mtp="0">Thistledown (R8) C</a>'
        }, {
            'id':
            'IEE',
            'html':
            '<a class="event_selector event-status-C" id="IEE" mtp="0">IE-Limerick GREY (R11) C</a>'
        }, {
            'id':
            'IOE',
            'html':
            '<a class="event_selector event-status-0" id="IOE" mtp="0">IE-Enniscorthy (R8) 0</a>'
        }, {
            'id':
            'VCD',
            'html':
            '<a class="event_selector event-status-0" id="VCD" mtp="0">Harrahs Philly (R13) 0</a>'
        }, {
            'id':
            'PRD',
            'html':
            '<a class="event_selector event-status-2" id="PRD" mtp="2">Plainridge (R3) 2</a>'
        }, {
            'id':
            'RCN',
            'html':
            '<a class="event_selector event-status-3" id="RCN" mtp="3">Rideau\n                                                            Carleton (R3) 3</a>'
        }, {
            'id':
            'GPM',
            'html':
            '<a class="event_selector event-status-4" id="GPM" mtp="4">Gulfstream (R13) 4</a>'
        }, {
            'id':
            'ARD',
            'html':
            '<a class="event_selector event-status-6" id="ARD" mtp="6">Arapahoe Thu (R5) 6</a>'
        }, {
            'id':
            'DLD',
            'html':
            '<a class="event_selector event-status-6" id="DLD" mtp="6">Delaware Park (R8) 6</a>'
        }, {
            'id':
            'WEM',
            'html':
            '<a class="event_selector event-status-6" id="WEM" mtp="6">Wheeling Mat Thu (R17) 6</a>'
        }, {
            'id':
            'MEE',
            'html':
            '<a class="event_selector event-status-8" id="MEE" mtp="8">Meadows (R13) 8</a>'
        }, {
            'id':
            'CON',
            'html':
            '<a class="event_selector event-status-21" id="CON" mtp="21">Charlottetwn Eve (R1) 21</a>'
        }, {
            'id':
            'DMD',
            'html':
            '<a class="event_selector event-status-21" id="DMD" mtp="21">Del\n                                                            Mar (R1) 21</a>'
        }, {
            'id':
            'STD',
            'html':
            '<a class="event_selector event-status-26" id="STD" mtp="26">Saratoga Tbd (R8) 26</a>'
        }, {
            'id':
            'PEN',
            'html':
            '<a class="event_selector event-status-05:00 PM" id="PEN" mtp="81">Penn National (R1) 05:00 PM</a>'
        }, {
            'id':
            'SGT',
            'html':
            '<a class="event_selector event-status-05:00 PM" id="SGT" mtp="81">Southland Twi (R1) 05:00 PM</a>'
        }, {
            'id':
            'CBD',
            'html':
            '<a class="event_selector event-status-05:12 PM" id="CBD" mtp="93">Canterbury Thu (R1) 05:12 PM</a>'
        }, {
            'id':
            'ETE',
            'html':
            '<a class="event_selector event-status-05:15 PM" id="ETE" mtp="96">Scioto Downs (R1) 05:15 PM</a>'
        }, {
            'id':
            'DBN',
            'html':
            '<a class="event_selector event-status-05:30 PM" id="DBN" mtp="111">Dubuque Eve (R1) 05:30 PM</a>'
        }, {
            'id':
            'EVN',
            'html':
            '<a class="event_selector event-status-05:50 PM" id="EVN" mtp="131">Evangeline (R1) 05:50 PM</a>'
        }, {
            'id':
            'TWN',
            'html':
            '<a class="event_selector event-status-06:00 PM" id="TWN" mtp="141">Charles Town (R1) 06:00 PM</a>'
        }, {
            'id':
            'TSE',
            'html':
            '<a class="event_selector event-status-06:00 PM" id="TSE" mtp="141">Tri State Eve (R1) 06:00 PM</a>'
        }, {
            'id':
            'WOH',
            'html':
            '<a class="event_selector event-status-06:00 PM" id="WOH" mtp="141">Woodbine Hrn (R1) 06:00 PM</a>'
        }, {
            'id':
            'AJE',
            'html':
            '<a class="event_selector event-status-06:05 PM" id="AJE" mtp="146">Running Aces (R1) 06:05 PM</a>'
        }, {
            'id':
            'YON',
            'html':
            '<a class="event_selector event-status-06:15 PM" id="YON" mtp="156">Yonkers (R1) 06:15 PM</a>'
        }, {
            'id':
            'EMD',
            'html':
            '<a class="event_selector event-status-07:58 PM" id="EMD" mtp="259">Emerald Downs (R1) 07:58 PM</a>'
        }, {
            'id':
            'U05',
            'html':
            '<a class="event_selector event-status-08:00 PM" id="U05" mtp="261">AU-Bendigo GH (R1) 08:00 PM</a>'
        }, {
            'id':
            'U6T',
            'html':
            '<a class="event_selector event-status-08:15 PM" id="U6T" mtp="276">AU-Goulburn GH (R1) 08:15 PM</a>'
        }, {
            'id':
            'U3D',
            'html':
            '<a class="event_selector event-status-08:30 PM" id="U3D" mtp="291">AU-Ipswich GH (R1) 08:30 PM</a>'
        }, {
            'id':
            'AUS',
            'html':
            '<a class="event_selector event-status-09:30 PM" id="AUS" mtp="351">AU-Australia - A (R1) 09:30 PM</a>'
        }, {
            'id':
            'AU1',
            'html':
            '<a class="event_selector event-status-09:37 PM" id="AU1" mtp="358">AU-Australia - H1 (R1) 09:37 PM</a>'
        }, {
            'id':
            'AUB',
            'html':
            '<a class="event_selector event-status-09:45 PM" id="AUB" mtp="366">AU-Australia - B (R1) 09:45 PM</a>'
        }, {
            'id':
            'AUC',
            'html':
            '<a class="event_selector event-status-09:52 PM" id="AUC" mtp="373">AU-Australia - C (R1) 09:52 PM</a>'
        }, {
            'id':
            'CAE',
            'html':
            '<a class="event_selector event-status-09:55 PM" id="CAE" mtp="376">MX-Caliente Eve (R1) 09:55 PM</a>'
        }, {
            'id':
            'M2V',
            'html':
            '<a class="event_selector event-status-10:00 PM" id="M2V" mtp="381">Pakenham Synthet (R1) 10:00 PM</a>'
        }, {
            'id':
            'AU2',
            'html':
            '<a class="event_selector event-status-10:33 PM" id="AU2" mtp="414">AU-Australia - H2 (R1) 10:33 PM</a>'
        }, {
            'id':
            'AUD',
            'html':
            '<a class="event_selector event-status-10:57 PM" id="AUD" mtp="438">AU-Australia - D (R1) 10:57 PM</a>'
        }, {
            'id':
            'KRB',
            'html':
            '<a class="event_selector event-status-11:25 PM" id="KRB" mtp="466">KR-Busan (R1) 11:25 PM</a>'
        }, {
            'id':
            'JP2',
            'html':
            '<a class="event_selector event-status-12:00 AM" id="JP2" mtp="501">JP-Kawasaki (R1) 12:00 AM</a>'
        }, {
            'id':
            'YSD',
            'html':
            '<a class="event_selector event-status-F" id="YSD" mtp="0">FR-Chateaubriant (R8) F</a>'
        }, {
            'id':
            'XVD',
            'html':
            '<a class="event_selector event-status-F" id="XVD" mtp="0">SA-Vaal (R8) F</a>'
        }, {
            'id':
            'FSD',
            'html':
            '<a class="event_selector event-status-F" id="FSD" mtp="0">UK-Ffos Las (R7) F</a>'
        }, {
            'id':
            'WOT',
            'html':
            '<a class="event_selector event-status-F" id="WOT" mtp="0">Woodbine Tbd (R1) F</a>'
        }, {
            'id':
            'GQD',
            'html':
            '<a class="event_selector event-status-F" id="GQD" mtp="0">UK-Chelmsford City (R7) F</a>'
        }, {
            'id':
            'VVD',
            'html':
            '<a class="event_selector event-status-F" id="VVD" mtp="0">FR-Divonne Les Bain (R8) F</a>'
        }, {
            'id':
            'CZD',
            'html':
            '<a class="event_selector event-status-F" id="CZD" mtp="0">UK-Carlisle (R7) F</a>'
        }, {
            'id':
            'XLD',
            'html':
            '<a class="event_selector event-status-F" id="XLD" mtp="0">FR-Deauville (R8) F</a>'
        }, {
            'id':
            'LFD',
            'html':
            '<a class="event_selector event-status-F" id="LFD" mtp="0">UK-Lingfield (R6) F</a>'
        }, {
            'id':
            'MRD',
            'html':
            '<a class="event_selector event-status-F" id="MRD" mtp="0">Monticello (R8) F</a>'
        }, {
            'id':
            'SSD',
            'html':
            '<a class="event_selector event-status-F" id="SSD" mtp="0">UK-Sedgefield (R7) F</a>'
        }, {
            'id':
            'TZD',
            'html':
            '<a class="event_selector event-status-F" id="TZD" mtp="0">IE-Tipperary (R8) F</a>'
        }, {
            'id':
            'YJD',
            'html':
            '<a class="event_selector event-status-F" id="YJD" mtp="0">FR-Cagnes-Sur-Mer (R7) F</a>'
        }, {
            'id':
            '2BD',
            'html':
            '<a class="event_selector event-status-F" id="2BD" mtp="0">Belterra Park (R8) F</a>'
        }, {
            'id':
            'I4E',
            'html':
            '<a class="event_selector event-status-F" id="I4E" mtp="0">IE-Newbridge (R11) F</a>'
        }, {
            'id':
            '2GE',
            'html':
            '<a class="event_selector event-status-F" id="2GE" mtp="0">UK-Sunderland GH UK (R12) F</a>'
        }, {
            'id':
            'UKN',
            'html':
            '<a class="event_selector event-status-F" id="UKN" mtp="0">UK-Monmore (R11) F</a>'
        }, {
            'id':
            '5GE',
            'html':
            '<a class="event_selector event-status-F" id="5GE" mtp="0">UK-Perry Barr GH UK (R12) F</a>'
        }, {
            'id':
            'UKK',
            'html':
            '<a class="event_selector event-status-F" id="UKK" mtp="0">UK-Hove Eve (R12) F</a>'
        }, {
            'id':
            'IGE',
            'html':
            '<a class="event_selector event-status-F" id="IGE" mtp="0">IE-Clonmel GREY (R9) F</a>'
        }]
        file_path = path.join(RES_PATH, 'amw_mtp_time.html')
        with open(file_path, 'r') as html:
            tracks = amwparser.get_track_list(html)
            self.assertEqual(tracks, expected)


if __name__ == '__main__':
    unittest.main()
