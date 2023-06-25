import re
import os
import copy
import logging


# noinspection PyBroadException
class Tokenizer:
    """
    Tokenizes strings into JSON objects.
    """

    rxPunc = re.compile('[^\\w ]')

    __slots__ = ('settings', 'tokenSplitRegexes', 'specialTokenRegexes')

    def __init__(self, settings: dict):
        self.settings = copy.deepcopy(settings)
        logging.basicConfig(filename=os.path.join("../Logs", "tokenizer.log"), filemode="w", level=logging.INFO)
        if 'non_word_internal_punct' not in self.settings:
            self.settings['non_word_internal_punct'] = ['\n', '\\n']
        self.tokenSplitRegexes = []
        self.specialTokenRegexes = []
        self.add_split_token_regexes()
        self.add_special_token_regexes()

    def add_split_token_regexes(self):
        """
        Add regexes that break certain spaceless tokens into parts.
        """
        if 'split_tokens' not in self.settings:
            return
        for str_rx in self.settings['split_tokens']:
            if not str_rx.startswith('^'):
                str_rx = '^' + str_rx
            if not str_rx.endswith('$'):
                str_rx += '$'
            try:
                self.tokenSplitRegexes.append(re.compile(str_rx))
            except:
                logging.error('Error when compiling a regex: ' + str_rx)

    def add_special_token_regexes(self):
        """
        Add regexes that recognize certain special tokens,
        such as email addresses or text-based smileys.
        """
        if 'special_tokens' not in self.settings:
            return
        for str_rx in self.settings['special_tokens']:
            try:
                self.specialTokenRegexes.append({'regex': re.compile(str_rx),
                                                 'token': self.settings['special_tokens'][str_rx]})
            except:
                print('Error when compiling a regex: ' + str_rx)

    @staticmethod
    def join_tokens(token_l: dict, token_r: dict):
        """
        Join tokenR to tokenL and make it a word.
        """
        token_l['wf'] += token_r['wf']
        token_l['off_end'] = token_r['off_end']
        token_l['wtype'] = 'word'

    def join_hyphens(self, tokens: list) -> list:
        """
        Take the list of tokens and join token segments like W-W.
        """
        if len(tokens) <= 0:
            return tokens
        joined_tokens = []
        for i in range(len(tokens)):
            token = copy.deepcopy(tokens[i])
            if len(joined_tokens) <= 0:
                joined_tokens.append(token)
                continue
            if (token['wtype'] == 'word'
                    and joined_tokens[-1]['wtype'] == 'word'
                    and joined_tokens[-1]['off_end'] == token['off_start']
                    and (len(self.tokenSplitRegexes) <= 0
                         or joined_tokens[-1]['wf'].endswith('-'))):
                self.join_tokens(joined_tokens[-1], token)
            elif (i < len(tokens) - 1 and
                  token['wtype'] == 'punct' and
                  token['wf'] not in self.settings['non_word_internal_punct'] and
                  (len(token['wf']) <= 0 or all(c not in self.settings['non_word_internal_punct']
                                                for c in token['wf'])) and
                  joined_tokens[-1]['wtype'] == 'word' and
                  tokens[i+1]['wtype'] == 'word' and
                  tokens[i]['off_start'] == joined_tokens[-1]['off_end'] and
                  tokens[i]['off_end'] == tokens[i+1]['off_start']):
                self.join_tokens(joined_tokens[-1], token)
            else:
                joined_tokens.append(token)
        return joined_tokens

    def add_token(self, tokens: list, token: dict):
        """
        Add one new token to the token list, taking into account that
        the settings may require splitting it into several parts.
        """
        if ('wtype' in token and token['wtype'] != 'word') or 'wf' not in token:
            tokens.append(token)
            return
        for r in self.tokenSplitRegexes:
            m = r.search(token['wf'])
            if m is not None:
                # print(token['wf'])
                for iGroup in range(1, 1 + len(m.groups())):
                    group = m.group(iGroup)
                    off_start, off_end = m.span(iGroup)
                    if group is not None and len(group) > 0 and off_start >= 0 and off_end >= 0:
                        new_token = copy.deepcopy(token)
                        new_token['off_end'] = new_token['off_start'] + off_end
                        new_token['off_start'] += off_start
                        new_token['wf'] = group
                        tokens.append(new_token)
                return
        tokens.append(token)

    def tokenize(self, text: str) -> list:
        tokens = []
        cur_token = {}
        i = -1
        while i < len(text) - 1:
            i += 1
            c = text[i]
            if c == ' ':
                if cur_token != {}:
                    cur_token['off_end'] = i
                    self.add_token(tokens, cur_token)
                    cur_token = {}
                continue
            if c == '\n':
                if cur_token != {}:
                    cur_token['off_end'] = i
                    self.add_token(tokens, cur_token)
                    cur_token = {}
                cur_token['wtype'] = 'punct'
                cur_token['off_start'] = i
                cur_token['off_end'] = i + 1
                cur_token['wf'] = '\\n'
                self.add_token(tokens, cur_token)
                cur_token = {}
                continue
            b_special_token_found = False
            for rx in self.specialTokenRegexes:
                m = rx['regex'].match(text, pos=i)
                if m is not None:
                    if cur_token != {}:
                        cur_token['off_end'] = i
                        self.add_token(tokens, cur_token)
                    cur_token = copy.deepcopy(rx['token'])
                    if 'wtype' not in cur_token:
                        cur_token['wtype'] = 'word'
                    wf = m.group(0)
                    if 'wf' not in cur_token:
                        cur_token['wf'] = wf
                    cur_token['off_start'] = i
                    cur_token['off_end'] = i + len(wf)
                    i += len(wf) - 1
                    self.add_token(tokens, cur_token)
                    cur_token = {}
                    b_special_token_found = True
                    break
            if b_special_token_found:
                continue
            if cur_token == {}:
                cur_token['off_start'] = i
                cur_token['wf'] = c
                if self.rxPunc.search(c) is not None:
                    cur_token['wtype'] = 'punct'
                else:
                    cur_token['wtype'] = 'word'
                continue
            b_punc = (self.rxPunc.search(c) is not None) or (c in self.settings['non_word_internal_punct'])
            if ((b_punc and cur_token['wtype'] == 'word') or
                    (not b_punc and cur_token['wtype'] == 'punct')):
                cur_token['off_end'] = i
                self.add_token(tokens, cur_token)
                cur_token = {'off_start': i, 'wf': c}
                if b_punc:
                    cur_token['wtype'] = 'punct'
                else:
                    cur_token['wtype'] = 'word'
                continue
            cur_token['wf'] += c
        if cur_token != {}:
            cur_token['off_end'] = len(text)
            self.add_token(tokens, cur_token)
        return self.join_hyphens(tokens)
