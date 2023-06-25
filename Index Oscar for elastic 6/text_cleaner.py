import re
import copy
import os
import logging
import html


class TextCleaner:
    """
    Contains methods for cleaning a string from things like
    HTML entities etc.
    """

    rxTags = re.compile('</?(?:a|img|span|div|p|body|html|head)(?: [^<>]+)?>|[\0⌐-♯]+', flags=re.DOTALL)
    rxSpaces1 = re.compile(' {2,}| +|\t+|&nbsp;| ', flags=re.DOTALL)
    rxSpaces2 = re.compile('(?: *\n)+ *', flags=re.DOTALL)
    rxPuncWords = re.compile('([,!?:;·;·)\\]>])([\\w(\\[<])')
    rxQuotesL = re.compile('([\\s(\\[{<\\-])"([\\w\\-\'`´‘’‛@.,-‒–—―•])', flags=re.DOTALL)
    rxQuotesR = re.compile('([\\w\\-\'`´‘’‛/@.,-‒–—―•,!?:;·;·])"([\\s)\\]}>\\-.,!])', flags=re.DOTALL)
    rxNonstandardQuotesL = re.compile('“', flags=re.DOTALL)
    rxNonstandardQuotesR = re.compile('”', flags=re.DOTALL)

    __slots__ = 'settings'

    def __init__(self, settings: dict):
        self.settings = copy.deepcopy(settings)

    def clean_text(self, text: str) -> str:
        """
        Main method that calls separate step-by-step procedures.
        """
        text = self.convert_html(text)
        text = self.clean_spaces(text)
        text = self.separate_words(text)
        if self.settings['convert_quotes']:
            text = self.convert_quotes(text=text, left_qm=self.settings['left_quot_mark'],
                                       right_qm=self.settings['right_quot_mark'])
        logging.basicConfig(filename=os.path.join("../Logs", "cleaner.log"), filemode="w", level=logging.INFO)
        text = self.clean_other(text)
        return text

    @staticmethod
    def convert_html(text: str) -> str:
        text = TextCleaner.rxTags.sub('', text)  # deletes all tags in angle brackets
        text = html.unescape(text)
        return text

    @staticmethod
    def clean_spaces(text: str) -> str:
        text = TextCleaner.rxSpaces1.sub(' ', text.strip())  # unify all spaces
        text = TextCleaner.rxSpaces2.sub('\n ', text)  # normalize new lines
        return text

    @staticmethod
    def separate_words(text: str) -> str:
        # punctuation inside a word
        text = TextCleaner.rxPuncWords.sub('\\1 \\2', text)  # adds a space between punctuation and next letter
        return text

    @staticmethod
    def convert_quotes(text: str, left_qm: str, right_qm: str) -> str:
        text = TextCleaner.rxQuotesL.sub('\\1«\\2', text)
        text = TextCleaner.rxQuotesR.sub('\\1»\\2', text)
        text = TextCleaner.rxNonstandardQuotesL.sub(left_qm, text)
        text = TextCleaner.rxNonstandardQuotesR.sub(right_qm, text)
        return text

    @staticmethod
    def clean_other(text: str) -> str:
        text = text.replace('…', '...')
        text = text.replace('\\r\\n', '\n')
        text = text.replace('\\n', '\n')
        text = text.replace('\\', '/')
        text = text.replace('\n', "")
        return text
