from model import get_complaint_data
import unittest


class GetComplaintDataTestCase(unittest.TestCase):

    def test_no_date_submitted(self):
        data = {
            "id": "a" * 32,
            "complaints": [
                {
                    "id": "0" * 32,
                }
            ]
        }
        result = list(get_complaint_data(data))
        self.assertEqual(result, [])

    def test_draft(self):
        data = {
            "id": "a" * 32,
            "complaints": [
                {
                    "id": "0" * 32,
                    "status": "draft",
                    "dateSubmitted": "sometimes"
                }
            ]
        }
        result = list(get_complaint_data(data))
        self.assertEqual(result, [])

    def test_claim(self):
        data = {
            "id": "a" * 32,
            "complaints": [
                {
                    "id": "0" * 32,
                    "type": "claim",
                    "dateSubmitted": "sometimes"
                }
            ]
        }
        result = list(get_complaint_data(data))
        self.assertEqual(result, [])

    def test_tender_complaints(self):
        data = {
            "id": "a" * 32,
            "unknown": 23,
            "complaints": [
                {
                    "id": "0" * 32,
                    "dateSubmitted": "sometimes",
                    "whatever": "whenever",
                }
            ],
            "cancellations": [
                {
                    "id": "b" * 32,
                    "reason": "got drunk, lost all the money",
                }
            ]
        }
        result = list(get_complaint_data(data))
        self.assertEqual(
            result,
            [
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '0' * 32,
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever'
                    },
                    'cancellations': [
                        {
                            'id': 'b' * 32,
                            'reason': 'got drunk, lost all the money'
                        }
                    ]
                }
            ]
        )

    def test_award_complaints(self):
        data = {
            "id": "a" * 32,
            "awards": [
                {
                    "complaints": [
                        {
                            "id": "0" * 32,
                            "dateSubmitted": "sometimes",
                            "whatever": "whenever",
                        }
                    ],
                }
            ]
        }
        result = list(get_complaint_data(data))
        self.assertEqual(
            result,
            [
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '0' * 32,
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever'
                    },
                    'cancellations': []
                }
            ]
        )

    def test_qualification_complaints(self):
        data = {
            "id": "a" * 32,
            "qualifications": [
                {
                    "complaints": [
                        {
                            "id": "0" * 32,
                            "dateSubmitted": "sometimes",
                            "whatever": "whenever",
                        }
                    ],
                }
            ]
        }
        result = list(get_complaint_data(data))
        self.assertEqual(
            result,
            [
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '0' * 32,
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever'
                    },
                    'cancellations': []
                }
            ]
        )

    def test_lots_complaints(self):
        data = {
            "id": "a" * 32,
            "complaints": [
                {
                    "id": "0" * 32,
                    "dateSubmitted": "sometimes",
                    "whatever": "whenever",
                    "relatedLot": 1,
                }
            ],
            "qualifications": [
                {
                    "complaints": [
                        {
                            "id": "0" * 32,
                            "dateSubmitted": "sometimes",
                            "whatever": "whenever",
                            "relatedLot": 2,
                        }
                    ],
                }
            ],
            "lots": [
                {
                    "id": 1,
                    "text": "hello",
                },
                {
                    "id": 2,
                    "text": "hello",
                }
            ],
            "cancellations": [
                {
                    "id": 4,
                    "relatedLot": 2,
                }
            ]

        }
        result = list(get_complaint_data(data))

        self.assertEqual(
            result,
            [
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '0' * 32,
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever', 'relatedLot': 1
                    },
                    'lot': {'id': 1},
                    'cancellations': []
                },
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '0' * 32,
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever',
                        'relatedLot': 2
                    },
                    'lot': {'id': 2},
                    'cancellations': [
                        {'id': 4, 'relatedLot': 2}
                    ]
                }
            ]
        )

    def test_lots_with_cancelled_tender(self):
        data = {
            "id": "a" * 32,
            "complaints": [
                {
                    "id": "0" * 32,
                    "dateSubmitted": "sometimes",
                    "whatever": "whenever",
                }
            ],
            "awards": [
                {
                    "complaints": [
                        {
                            "id": "1" * 32,
                            "dateSubmitted": "sometimes",
                            "whatever": "whenever",
                            "relatedLot": 2,
                        }
                    ],
                }
            ],
            "lots": [
                {
                    "id": 1,
                    "text": "hello",
                },
                {
                    "id": 2,
                    "text": "hello",
                }
            ],
            "cancellations": [
                {
                    "id": 4,
                    "reason": "reason",
                }
            ]

        }
        result = list(get_complaint_data(data))

        self.assertEqual(
            result,
            [
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '00000000000000000000000000000000',
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever'
                    },
                    'cancellations': [
                        {'id': 4, 'reason': 'reason'}
                    ]
                },
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '1' * 32,
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever',
                        'relatedLot': 2
                    },
                    'lot': {'id': 2},
                    'cancellations': [
                        {'id': 4, 'reason': 'reason'}
                    ]
                }
            ]
        )

    def test_award_lot(self):
        data = {
            "id": "a" * 32,
            "awards": [
                {
                    "lotID": "13",
                    "complaints": [
                        {
                            "id": "1" * 32,
                            "dateSubmitted": "sometimes",
                            "whatever": "whenever",
                            "relatedLot": 2,
                        }
                    ],
                }
            ],
            "lots": [
                {
                    "id": 2,
                    "text": "hello",
                },
                {
                    "id": "13",
                    "text": "room",
                }
            ]

        }
        result = list(get_complaint_data(data))

        self.assertEqual(
            result,
            [
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '1' * 32,
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever',
                        'relatedLot': '13'
                    },
                    'lot': {'id': '13'},
                    'cancellations': []
                }
            ]
        )

    def test_qualification_lot(self):
        data = {
            "id": "a" * 32,
            "qualifications": [
                {
                    "lotID": "13",
                    "complaints": [
                        {
                            "id": "1" * 32,
                            "dateSubmitted": "sometimes",
                            "whatever": "whenever",
                            "relatedLot": 2,
                        }
                    ],
                }
            ],
            "lots": [
                {
                    "id": 2,
                    "text": "hello",
                },
                {
                    "id": "13",
                    "text": "room",
                }
            ]

        }
        result = list(get_complaint_data(data))

        self.assertEqual(
            result,
            [
                {
                    'tender': {'id': 'a' * 32},
                    'complaint': {
                        'id': '1' * 32,
                        'dateSubmitted': 'sometimes',
                        'whatever': 'whenever',
                        'relatedLot': '13'
                    },
                    'lot': {'id': '13'},
                    'cancellations': []
                }
            ]
        )
