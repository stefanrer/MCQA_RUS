import re
import copy
import os
import logging


# noinspection PyBroadException
class Splitter:
    """
    Contains methods for splitting list of tokens
    into sentences.
    """

    __slots__ = ('settings', 'rxSentEnd', 'rxSentStart', 'rxPuncTransparent')

    def __init__(self, settings: dict):
        self.settings = copy.deepcopy(settings)
        logging.basicConfig(filename=os.path.join("../Logs", "splitter.log"), filemode="w",
                            level=logging.INFO)
        try:
            self.rxSentEnd = re.compile(self.settings['sent_end_punc'])
        except:
            logging.error('Please check your sentence end regexp.')
            self.rxSentEnd = re.compile('[.?!]')
        try:
            self.rxSentStart = re.compile(self.settings['sent_start'])
        except:
            logging.error('Please check your sentence start regexp.')
            self.rxSentStart = re.compile('[A-ZА-ЯЁ]')
        # "Transparent punctuation" is punctuation that should not be counted
        # when calculating distances between words.
        if 'transparent_punctuation' in self.settings:
            try:
                self.rxPuncTransparent = re.compile(self.settings['transparent_punctuation'])
            except:
                logging.error('Please check your transparent punctuation regexp.')
                self.rxPuncTransparent = re.compile('^ *$')
        else:
            self.rxPuncTransparent = re.compile('^ *$')

    @staticmethod
    def join_sentences(sentence_l: dict, sentence_r: dict, absolute_offsets: bool = False):
        """
        Add the words and the text of sentenceR to sentenceL.
        If absolute_offsets == True, treat all start and end offsets as referring
        to the whole text rather than to the corresponding sentences.
        The operation may change sentenceR (it is assumed that sentenceR
        is not used anymore after this function has been called).
        """
        if len(sentence_r['words']) <= 0:
            return
        if absolute_offsets:
            n_spaces_between = sentence_r['words'][0]['off_start'] - sentence_l['words'][-1]['off_end']
            start_offset_shift_r = 0
        else:
            n_spaces_between = 1  # Default: one space between sentences
            start_offset_shift_r = len(sentence_l['text']) + 1
            for word in sentence_r['words']:
                word['off_start'] += start_offset_shift_r
                word['off_end'] += start_offset_shift_r
        sentence_l['words'] += sentence_r['words']
        sentence_l['text'] += ' ' * n_spaces_between + sentence_r['text']

        # Now, shift all character offsets in source alignment etc.
        for segType in ['src_alignment', 'para_alignment', 'style_spans']:
            if segType in sentence_r:
                if segType not in sentence_l:
                    sentence_l[segType] = []
                for seg in sentence_r[segType]:
                    for key in ['off_start', 'off_end', 'off_start_sent', 'off_end_sent']:
                        if key in seg:
                            seg[key] += start_offset_shift_r
                    sentence_l[segType].append(seg)

    def append_sentence(self, sentences: list, s: dict, text: str):
        """
        Append a sentence to the list of sentences. If it is
        not a real sentences, just add all of its tokens to the
        last sentence of the list.
        """
        if len(s['words']) == 0:
            s['text'] = ''
        else:
            start_offset = s['words'][0]['off_start']
            end_offset = s['words'][-1]['off_end']
            s['text'] = text[start_offset:end_offset]
        if len(s['words']) == 0:
            return
        if len(sentences) > 0 and all(w['wtype'] == 'punct'
                                      for w in s['words']):
            self.join_sentences(sentences[-1], s, absolute_offsets=True)
        else:
            sentences.append(s)

    @staticmethod
    def next_word(tokens: list, start_num: int) -> str:
        """
        Find the nearest wordform to the right of startNum,
        including startNum itself. Return its string value.
        """
        for i in range(start_num, len(tokens)):
            if tokens[i]['wtype'] == 'word':
                return tokens[i]['wf']
        return ''

    @staticmethod
    def recalculate_offsets_sentence(sent: dict):
        """
        Recalculate offsets in a single sentence
        so that they start at the beginning of the sentence.
        """
        if len(sent['words']) <= 0:
            return
        start_offset = sent['words'][0]['off_start']
        for w in sent['words']:
            w['off_start'] -= start_offset
            w['off_end'] -= start_offset

    def recalculate_offsets(self, sentences: list):
        """
        Recalculate offsets so that they always start at the
        beginning of the sentence.
        """
        for sent in sentences:
            self.recalculate_offsets_sentence(sent)

    def add_next_word_id_sentence(self, sent: dict):
        """
        Insert the ID of the next word in a single sentence. (This is important for
        the sentences that can have multiple tokenization variants.)
        Assign both forward and backward numbers.
        """
        if len(sent['words']) <= 0:
            return
        words = sent['words']
        # Forward numbering and next word ID (LTR)
        leading_punct = 0
        max_word_num = 0
        words_started = False
        for i in range(len(words)):
            if not words_started:
                if words[i]['wtype'] != 'word':
                    leading_punct += 1
                else:
                    words_started = True
            if words[i]['wtype'] not in ['style_span']:
                words[i]['next_word'] = i + 1
            if words_started and not (all(words[j]['wtype'] != 'word' for j in range(i, len(words)))):
                if words[i]['wtype'] == 'word' or self.rxPuncTransparent.search(words[i]['wf']) is None:
                    words[i]['sentence_index'] = i - leading_punct
                    max_word_num = i - leading_punct
                else:
                    leading_punct += 1

        # Backward numbering
        if max_word_num > 0:
            for i in range(len(words)):
                if 'sentence_index' in words[i]:
                    words[i]['sentence_index_neg'] = max_word_num - words[i]['sentence_index']

    def add_next_word_id(self, sentences: list):
        """
        Insert the ID of the next word. (This is important for
        the sentences that can have multiple tokenization variants.)
        """
        for sent in sentences:
            self.add_next_word_id_sentence(sent)

    def split(self, tokens: list, text: str) -> list:
        """
        Split the text into sentences by packing tokens into
        separate sentence JSON objects.
        Return the resulting list of sentences.
        """
        sentences = []
        cur_sentence = {'words': []}
        for i in range(len(tokens)):
            wf = tokens[i]['wf']
            cur_sentence['words'].append(tokens[i])
            if tokens[i]['wtype'] == 'punct':
                if (i == len(tokens) - 1
                        or (self.settings['newline_ends_sent'] and wf == '\\n')
                        or (self.rxSentEnd.search(wf) is not None
                            and i > 0
                            and tokens[i - 1]['wf'] not in self.settings['abbreviations']
                            and self.rxSentStart.search(self.next_word(tokens, i + 1)) is not None)):
                    self.append_sentence(sentences, cur_sentence, text)
                    cur_sentence = {'words': []}
                    continue
            elif i == len(tokens) - 1:
                self.append_sentence(sentences, cur_sentence, text)
        self.recalculate_offsets(sentences)
        self.add_next_word_id(sentences)
        return sentences
