"""Unit tests for the `diff` server-side verb.

Covers `python_modules/diff.py`:
- BinaryAwareEncoder: bytes → base64 JSON encoding
- SegmentStream: parallel-scan stream abstraction (pagination, peek, advance, key extraction)
- item_matches: JSON-based item comparison
- format_item_with_keys_first: key ordering for display
- log_diff: concise vs full format output
- diff_segment: core segment diffing logic (same pk, different pk, alignment, s3 output)
- run(): argument wiring, sampling, schema broadcast, result printing
"""

import base64
import json
import sys
from unittest.mock import MagicMock, patch, call

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
_pyspark_sql = MagicMock()
sys.modules.setdefault('pyspark.sql', _pyspark_sql)
_pyspark_sql_functions = MagicMock()
sys.modules.setdefault('pyspark.sql.functions', _pyspark_sql_functions)

from python_modules import diff as diff_module

if not hasattr(diff_module, 'get_error_message'):
    diff_module.get_error_message = lambda e: str(e)


# --- BinaryAwareEncoder --------------------------------------------------------

class TestBinaryAwareEncoder:

    def test_encodes_bytes_as_base64(self):
        data = b'\x00\x01\x02\xff'
        result = json.dumps(data, cls=diff_module.BinaryAwareEncoder)
        expected = json.dumps(base64.b64encode(data).decode('utf-8'))
        assert result == expected

    def test_passes_through_non_bytes(self):
        data = {"key": "value", "num": 42, "flag": True}
        result = json.dumps(data, cls=diff_module.BinaryAwareEncoder)
        assert result == json.dumps(data)

    def test_raises_for_unsupported_types(self):
        with pytest.raises(TypeError):
            json.dumps(object(), cls=diff_module.BinaryAwareEncoder)

    def test_handles_nested_bytes_in_dict(self):
        data = {"bin": b'\xde\xad'}
        result = json.loads(json.dumps(data, cls=diff_module.BinaryAwareEncoder))
        assert result["bin"] == base64.b64encode(b'\xde\xad').decode('utf-8')


# --- SegmentStream -------------------------------------------------------------

class TestSegmentStream:

    def _make_stream(self, items_pages, pk='id', sk=None):
        """Create a SegmentStream with a mocked DynamoDB client that returns pages."""
        session = MagicMock()
        client = MagicMock()
        session.client.return_value = client

        self._page_index = 0
        pages = items_pages

        def mock_scan(**kwargs):
            if self._page_index >= len(pages):
                return {'Items': []}
            page = pages[self._page_index]
            self._page_index += 1
            resp = {'Items': page}
            if self._page_index < len(pages):
                resp['LastEvaluatedKey'] = {'id': {'S': 'marker'}}
            return resp

        client.scan.side_effect = mock_scan

        stream = diff_module.SegmentStream(
            session=session,
            table_name='test-table',
            segment=0,
            total_segments=1,
            consistent_read=False,
            pk=pk,
            sk=sk
        )
        return stream

    def test_head_returns_first_item(self):
        items = [[{'id': {'S': 'a'}, 'val': {'N': '1'}}]]
        stream = self._make_stream(items)
        assert stream.head() == {'id': {'S': 'a'}, 'val': {'N': '1'}}

    def test_head_returns_none_when_empty(self):
        stream = self._make_stream([[]])
        assert stream.head() is None

    def test_head_pk_extracts_pk_value(self):
        items = [[{'id': {'S': 'pk_val'}, 'data': {'S': 'x'}}]]
        stream = self._make_stream(items, pk='id')
        assert stream.head_pk() == 'pk_val'

    def test_head_sk_returns_none_without_sort_key(self):
        items = [[{'id': {'S': 'a'}}]]
        stream = self._make_stream(items, pk='id', sk=None)
        assert stream.head_sk() is None

    def test_head_sk_extracts_sk_value(self):
        items = [[{'pk': {'S': 'a'}, 'sk': {'S': 'sort_val'}}]]
        stream = self._make_stream(items, pk='pk', sk='sk')
        assert stream.head_sk() == 'sort_val'

    def test_head_key_pk_only(self):
        items = [[{'id': {'S': 'a'}, 'extra': {'S': 'x'}}]]
        stream = self._make_stream(items, pk='id')
        assert stream.head_key() == {'id': {'S': 'a'}}

    def test_head_key_pk_and_sk(self):
        items = [[{'pk': {'S': 'a'}, 'sk': {'N': '1'}, 'extra': {'S': 'x'}}]]
        stream = self._make_stream(items, pk='pk', sk='sk')
        assert stream.head_key() == {'pk': {'S': 'a'}, 'sk': {'N': '1'}}

    def test_head_key_returns_none_when_empty(self):
        stream = self._make_stream([[]])
        assert stream.head_key() is None

    def test_advance_removes_first_item(self):
        items = [[{'id': {'S': 'a'}}, {'id': {'S': 'b'}}]]
        stream = self._make_stream(items)
        assert stream.head_pk() == 'a'
        stream.advance()
        assert stream.head_pk() == 'b'

    def test_advance_on_empty_does_not_raise(self):
        stream = self._make_stream([[]])
        stream.advance()

    def test_is_finished_when_empty_and_last_page(self):
        stream = self._make_stream([[]])
        stream.head()  # trigger load
        assert stream.is_finished()

    def test_is_finished_false_with_items(self):
        items = [[{'id': {'S': 'a'}}]]
        stream = self._make_stream(items)
        stream.head()  # trigger load
        assert not stream.is_finished()

    def test_pagination_loads_next_page(self):
        page1 = [{'id': {'S': 'a'}}]
        page2 = [{'id': {'S': 'b'}}]
        stream = self._make_stream([page1, page2])
        stream.head()  # loads page1
        stream.advance()
        assert stream.head_pk() == 'b'

    def test_peek_loads_items_on_demand(self):
        page1 = [{'id': {'S': 'a'}}]
        page2 = [{'id': {'S': 'b'}}]
        stream = self._make_stream([page1, page2])
        assert stream.peek(1) == {'id': {'S': 'b'}}

    def test_peek_pk_returns_none_beyond_data(self):
        items = [[{'id': {'S': 'a'}}]]
        stream = self._make_stream(items)
        assert stream.peek_pk(5) is None

    def test_peek_sk_returns_none_beyond_data(self):
        items = [[{'pk': {'S': 'a'}, 'sk': {'S': 'b'}}]]
        stream = self._make_stream(items, pk='pk', sk='sk')
        assert stream.peek_sk(5) is None

    def test_peek_sk_returns_none_without_sk_configured(self):
        items = [[{'id': {'S': 'a'}}]]
        stream = self._make_stream(items, pk='id', sk=None)
        assert stream.peek_sk(0) is None

    def test_consistent_read_passed_to_scan(self):
        session = MagicMock()
        client = MagicMock()
        client.scan.return_value = {'Items': [{'id': {'S': 'a'}}]}
        session.client.return_value = client

        stream = diff_module.SegmentStream(session, 'tbl', 3, 10, True, 'id', None)
        stream.head()

        call_kwargs = client.scan.call_args.kwargs
        assert call_kwargs['ConsistentRead'] is True
        assert call_kwargs['Segment'] == 3
        assert call_kwargs['TotalSegments'] == 10

    def test_exclusive_start_key_used_on_subsequent_pages(self):
        session = MagicMock()
        client = MagicMock()
        client.scan.side_effect = [
            {'Items': [{'id': {'S': 'a'}}], 'LastEvaluatedKey': {'id': {'S': 'a'}}},
            {'Items': [{'id': {'S': 'b'}}]},
        ]
        session.client.return_value = client

        stream = diff_module.SegmentStream(session, 'tbl', 0, 1, False, 'id', None)
        stream.peek(1)  # force loading second page

        second_call_kwargs = client.scan.call_args_list[1].kwargs
        assert second_call_kwargs['ExclusiveStartKey'] == {'id': {'S': 'a'}}

    def test_load_page_stops_after_last_page(self):
        session = MagicMock()
        client = MagicMock()
        client.scan.return_value = {'Items': [{'id': {'S': 'a'}}]}
        session.client.return_value = client

        stream = diff_module.SegmentStream(session, 'tbl', 0, 1, False, 'id', None)
        stream.head()
        stream.peek(10)  # try to load beyond last page
        assert client.scan.call_count == 1


# --- item_matches --------------------------------------------------------------

class TestItemMatches:

    def test_identical_items_match(self):
        item = {'pk': {'S': 'a'}, 'val': {'N': '1'}}
        assert diff_module.item_matches(item, item)

    def test_different_items_do_not_match(self):
        a = {'pk': {'S': 'a'}, 'val': {'N': '1'}}
        b = {'pk': {'S': 'a'}, 'val': {'N': '2'}}
        assert not diff_module.item_matches(a, b)

    def test_key_order_does_not_affect_match(self):
        a = {'z': {'S': '1'}, 'a': {'S': '2'}}
        b = {'a': {'S': '2'}, 'z': {'S': '1'}}
        assert diff_module.item_matches(a, b)

    def test_binary_values_compared_correctly(self):
        a = {'pk': {'S': 'k'}, 'data': {'B': b'\x00\x01'}}
        b = {'pk': {'S': 'k'}, 'data': {'B': b'\x00\x01'}}
        assert diff_module.item_matches(a, b)

    def test_different_binary_values_do_not_match(self):
        a = {'pk': {'S': 'k'}, 'data': {'B': b'\x00\x01'}}
        b = {'pk': {'S': 'k'}, 'data': {'B': b'\x00\x02'}}
        assert not diff_module.item_matches(a, b)


# --- format_item_with_keys_first -----------------------------------------------

class TestFormatItemWithKeysFirst:

    def test_pk_first(self):
        item = {'z': {'S': '3'}, 'a': {'S': '1'}, 'pk': {'S': 'val'}}
        result = diff_module.format_item_with_keys_first(item, 'pk')
        keys = list(result.keys())
        assert keys[0] == 'pk'

    def test_pk_and_sk_first(self):
        item = {'z': {'S': '3'}, 'sk': {'S': '2'}, 'pk': {'S': '1'}, 'a': {'S': '0'}}
        result = diff_module.format_item_with_keys_first(item, 'pk', 'sk')
        keys = list(result.keys())
        assert keys[0] == 'pk'
        assert keys[1] == 'sk'

    def test_remaining_keys_sorted(self):
        item = {'pk': {'S': '1'}, 'z': {'S': '3'}, 'a': {'S': '2'}, 'm': {'S': '4'}}
        result = diff_module.format_item_with_keys_first(item, 'pk')
        keys = list(result.keys())
        assert keys == ['pk', 'a', 'm', 'z']

    def test_no_sk_param_skips_sk_ordering(self):
        item = {'pk': {'S': '1'}, 'sk': {'S': '2'}, 'b': {'S': '3'}}
        result = diff_module.format_item_with_keys_first(item, 'pk')
        keys = list(result.keys())
        assert keys[0] == 'pk'
        assert 'b' in keys
        assert 'sk' in keys


# --- log_diff ------------------------------------------------------------------

class TestLogDiff:

    def _make_stream_with_item(self, item, pk='pk', sk=None):
        """Return a mock stream with .head() returning the given item."""
        stream = MagicMock()
        stream.head.return_value = item
        stream.pk = pk
        stream.sk = sk
        stream.head_key.return_value = {pk: item[pk]} if item else None
        if sk and item and sk in item:
            stream.head_key.return_value[sk] = item[sk]
        return stream

    def test_concise_format_shows_keys_only(self):
        item = {'pk': {'S': 'a'}, 'sk': {'S': 'b'}, 'data': {'S': 'val'}}
        stream = self._make_stream_with_item(item, pk='pk', sk='sk')
        result = diff_module.log_diff('-', stream, True)
        assert result.startswith('-')
        assert 'data' not in result

    def test_full_format_shows_all_attributes(self):
        item = {'pk': {'S': 'a'}, 'data': {'S': 'val'}}
        stream = self._make_stream_with_item(item, pk='pk')
        result = diff_module.log_diff('+', stream, False)
        assert result.startswith('+')
        assert 'data' in result
        assert 'val' in result

    def test_returns_empty_string_for_none_item(self):
        stream = MagicMock()
        stream.head.return_value = None
        result = diff_module.log_diff('-', stream, True)
        assert result == ''

    def test_binary_value_encoded_in_output(self):
        item = {'pk': {'S': 'a'}, 'bin': {'B': b'\xde\xad'}}
        stream = self._make_stream_with_item(item, pk='pk')
        result = diff_module.log_diff('-', stream, False)
        expected_b64 = base64.b64encode(b'\xde\xad').decode('utf-8')
        assert expected_b64 in result

    def test_symbol_prefix_used(self):
        item = {'pk': {'S': 'x'}}
        stream = self._make_stream_with_item(item, pk='pk')
        result = diff_module.log_diff('*', stream, True)
        assert result.startswith('* ')


# --- diff_segment --------------------------------------------------------------

class TestDiffSegment:

    def _make_schema_broadcast(self, pk1='pk', sk1=None, pk2='pk', sk2=None):
        broadcast = MagicMock()
        broadcast.value = {
            'table1': {'pk': pk1, 'sk': sk1},
            'table2': {'pk': pk2, 'sk': sk2}
        }
        return broadcast

    def _make_rate_limiter_config(self):
        return MagicMock()

    def _make_monitor_options(self):
        return {}

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_identical_tables_no_diffs(self, mock_stream_cls, mock_rl):
        """Two identical single-item streams produce no diffs."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item = {'pk': {'S': 'a'}, 'val': {'N': '1'}}

        items_a = [item]
        items_b = [item]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: None
        stream_a.has_sort_key = False
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = None

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: None
        stream_b.has_sort_key = False
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), self._make_rate_limiter_config()
        )
        assert result == []

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_item_in_a_not_in_b_reported_as_minus(self, mock_stream_cls, mock_rl):
        """Item in stream_a but not stream_b appears as '-'."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_a = {'pk': {'S': 'a'}, 'val': {'N': '1'}}

        items_a = [item_a]
        idx_a = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: None
        stream_a.head_key = lambda: {'pk': {'S': 'a'}} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = False
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = None
        stream_a.peek = lambda n=0: items_a[idx_a[0] + n] if (idx_a[0] + n) < len(items_a) else None
        stream_a.peek_pk = lambda n: 'a' if (idx_a[0] + n) < len(items_a) else None

        stream_b.head = lambda: None
        stream_b.head_pk = lambda: None
        stream_b.head_sk = lambda: None
        stream_b.has_sort_key = False
        stream_b.is_finished = lambda: True
        stream_b.advance = lambda: None
        stream_b.pk = 'pk'
        stream_b.sk = None
        stream_b.peek = lambda n=0: None
        stream_b.peek_pk = lambda n: None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), self._make_rate_limiter_config()
        )
        assert len(result) == 1
        assert result[0].startswith('-')

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_item_in_b_not_in_a_reported_as_plus(self, mock_stream_cls, mock_rl):
        """Item in stream_b but not stream_a appears as '+'."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_b = {'pk': {'S': 'b'}, 'val': {'N': '2'}}

        items_b = [item_b]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: None
        stream_a.head_pk = lambda: None
        stream_a.has_sort_key = False
        stream_a.is_finished = lambda: True
        stream_a.advance = lambda: None
        stream_a.pk = 'pk'
        stream_a.sk = None

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'b' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: None
        stream_b.head_key = lambda: {'pk': {'S': 'b'}} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = False
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), self._make_rate_limiter_config()
        )
        assert len(result) == 1
        assert result[0].startswith('+')

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_changed_item_reported_as_star_concise(self, mock_stream_cls, mock_rl):
        """Same pk but different values in concise mode → '*' diff."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_a = {'pk': {'S': 'a'}, 'val': {'N': '1'}}
        item_b = {'pk': {'S': 'a'}, 'val': {'N': '2'}}

        items_a = [item_a]
        items_b = [item_b]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: None
        stream_a.head_key = lambda: {'pk': {'S': 'a'}} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = False
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = None

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: None
        stream_b.head_key = lambda: {'pk': {'S': 'a'}} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = False
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), self._make_rate_limiter_config()
        )
        assert len(result) == 1
        assert result[0].startswith('*')

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_changed_item_full_format_shows_minus_plus(self, mock_stream_cls, mock_rl):
        """Same pk but different values in full mode → '-' then '+' lines."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_a = {'pk': {'S': 'a'}, 'val': {'N': '1'}}
        item_b = {'pk': {'S': 'a'}, 'val': {'N': '2'}}

        items_a = [item_a]
        items_b = [item_b]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: None
        stream_a.head_key = lambda: {'pk': {'S': 'a'}} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = False
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = None

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: None
        stream_b.head_key = lambda: {'pk': {'S': 'a'}} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = False
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, False, 'job1', False, None,
            self._make_schema_broadcast(), self._make_rate_limiter_config()
        )
        assert len(result) == 2
        assert result[0].startswith('-')
        assert result[1].startswith('+')

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_with_sort_key_same_pk_different_sk(self, mock_stream_cls, mock_rl):
        """Same pk, sk in A not in B → '-'."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_a1 = {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}}
        item_a2 = {'pk': {'S': 'a'}, 'sk': {'S': '2'}, 'val': {'N': '20'}}
        item_b1 = {'pk': {'S': 'a'}, 'sk': {'S': '2'}, 'val': {'N': '20'}}

        items_a = [item_a1, item_a2]
        items_b = [item_b1]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: items_a[idx_a[0]]['sk']['S'] if idx_a[0] < len(items_a) else None
        stream_a.head_key = lambda: {'pk': items_a[idx_a[0]]['pk'], 'sk': items_a[idx_a[0]]['sk']} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = True
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = 'sk'
        stream_a.peek = lambda n=0: items_a[idx_a[0] + n] if (idx_a[0] + n) < len(items_a) else None
        stream_a.peek_pk = lambda n: 'a' if (idx_a[0] + n) < len(items_a) else None

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: items_b[idx_b[0]]['sk']['S'] if idx_b[0] < len(items_b) else None
        stream_b.head_key = lambda: {'pk': items_b[idx_b[0]]['pk'], 'sk': items_b[idx_b[0]]['sk']} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = True
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = 'sk'
        stream_b.peek = lambda n=0: items_b[idx_b[0] + n] if (idx_b[0] + n) < len(items_b) else None
        stream_b.peek_pk = lambda n: 'a' if (idx_b[0] + n) < len(items_b) else None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(pk1='pk', sk1='sk', pk2='pk', sk2='sk'),
            self._make_rate_limiter_config()
        )
        assert any(r.startswith('-') for r in result)

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_with_sort_key_extra_in_b(self, mock_stream_cls, mock_rl):
        """Same pk, extra sk in B → '+'."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_a1 = {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}}
        item_b1 = {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}}
        item_b2 = {'pk': {'S': 'a'}, 'sk': {'S': '3'}, 'val': {'N': '30'}}

        items_a = [item_a1]
        items_b = [item_b1, item_b2]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: items_a[idx_a[0]]['sk']['S'] if idx_a[0] < len(items_a) else None
        stream_a.head_key = lambda: {'pk': items_a[idx_a[0]]['pk'], 'sk': items_a[idx_a[0]]['sk']} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = True
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = 'sk'
        stream_a.peek = lambda n=0: items_a[idx_a[0] + n] if (idx_a[0] + n) < len(items_a) else None
        stream_a.peek_pk = lambda n: 'a' if (idx_a[0] + n) < len(items_a) else None

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: items_b[idx_b[0]]['sk']['S'] if idx_b[0] < len(items_b) else None
        stream_b.head_key = lambda: {'pk': items_b[idx_b[0]]['pk'], 'sk': items_b[idx_b[0]]['sk']} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = True
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = 'sk'
        stream_b.peek = lambda n=0: items_b[idx_b[0] + n] if (idx_b[0] + n) < len(items_b) else None
        stream_b.peek_pk = lambda n: 'a' if (idx_b[0] + n) < len(items_b) else None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(pk1='pk', sk1='sk', pk2='pk', sk2='sk'),
            self._make_rate_limiter_config()
        )
        plus_lines = [r for r in result if r.startswith('+')]
        assert len(plus_lines) == 1

    @patch.object(diff_module, 'boto3')
    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_s3_output_puts_object(self, mock_stream_cls, mock_rl, mock_boto3):
        """When use_s3=True, diff is written to S3 and count returned."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_a = {'pk': {'S': 'a'}, 'val': {'N': '1'}}

        items_a = [item_a]
        idx_a = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: None
        stream_a.head_key = lambda: {'pk': {'S': 'a'}} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = False
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = None

        stream_b.head = lambda: None
        stream_b.head_pk = lambda: None
        stream_b.has_sort_key = False
        stream_b.is_finished = lambda: True
        stream_b.advance = lambda: None
        stream_b.pk = 'pk'
        stream_b.sk = None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        s3_client = MagicMock()
        mock_boto3.client.return_value = s3_client

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            5, 10, False, True, 'job123', True, 'my-bucket',
            self._make_schema_broadcast(), self._make_rate_limiter_config()
        )
        assert result == 1
        s3_client.put_object.assert_called_once()
        put_kwargs = s3_client.put_object.call_args.kwargs
        assert put_kwargs['Bucket'] == 'my-bucket'
        assert put_kwargs['Key'] == 'job123/5.txt'

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_output_truncated_to_print_limit(self, mock_stream_cls, mock_rl):
        """Without S3, result is truncated to PRINT_LIMIT."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        num_items = 200
        items_a = [{'pk': {'S': str(i)}, 'val': {'N': str(i)}} for i in range(num_items)]
        idx_a = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: str(idx_a[0]) if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: None
        stream_a.head_key = lambda: {'pk': items_a[idx_a[0]]['pk']} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = False
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = None

        stream_b.head = lambda: None
        stream_b.head_pk = lambda: None
        stream_b.has_sort_key = False
        stream_b.is_finished = lambda: True
        stream_b.advance = lambda: None
        stream_b.pk = 'pk'
        stream_b.sk = None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), self._make_rate_limiter_config()
        )
        assert len(result) == diff_module.PRINT_LIMIT

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_rate_limiter_shutdown_called(self, mock_stream_cls, mock_rl):
        """Rate limiter workers are shut down in finally block."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        stream_a = MagicMock()
        stream_b = MagicMock()
        stream_a.is_finished.return_value = True
        stream_a.head.return_value = None
        stream_b.is_finished.return_value = True
        stream_b.head.return_value = None
        stream_a.has_sort_key = False
        stream_b.has_sort_key = False
        stream_a.pk = 'pk'
        stream_b.pk = 'pk'

        mock_stream_cls.side_effect = [stream_a, stream_b]

        diff_module.diff_segment(
            'table1', 'table2',
            self._make_monitor_options(), self._make_monitor_options(),
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), self._make_rate_limiter_config()
        )
        assert mock_rl_instance.shutdown.call_count == 2

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_rate_limiter_shutdown_on_exception(self, mock_stream_cls, mock_rl):
        """Rate limiter workers are shut down even when an exception occurs."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        mock_stream_cls.side_effect = RuntimeError("stream creation failed")

        with pytest.raises(RuntimeError):
            diff_module.diff_segment(
                'table1', 'table2',
                self._make_monitor_options(), self._make_monitor_options(),
                0, 1, False, True, 'job1', False, None,
                self._make_schema_broadcast(), self._make_rate_limiter_config()
            )
        assert mock_rl_instance.shutdown.call_count == 2


# --- print_dynamodb_table_info -------------------------------------------------

class TestPrintDynamodbTableInfo:

    @patch.object(diff_module, 'get_and_print_dynamodb_table_info')
    @patch.object(diff_module, 'get_and_print_table_scan_cost', return_value=1.50)
    @patch.object(diff_module, 'boto3')
    def test_returns_scan_cost_and_item_count(self, mock_boto3, mock_scan_cost, mock_table_info):
        mock_boto3.Session.return_value.region_name = 'us-east-1'
        mock_table_info.return_value = {'item_count': 100}

        result = diff_module.print_dynamodb_table_info('my-table')
        assert result == (1.50, 100)
        mock_table_info.assert_called_once_with('my-table')

    @patch.object(diff_module, 'get_and_print_dynamodb_table_info')
    @patch.object(diff_module, 'get_and_print_table_scan_cost', return_value=0.75)
    @patch.object(diff_module, 'boto3')
    def test_passes_fraction(self, mock_boto3, mock_scan_cost, mock_table_info):
        mock_boto3.Session.return_value.region_name = 'us-west-2'
        mock_table_info.return_value = {'item_count': 50}

        diff_module.print_dynamodb_table_info('tbl', fraction=0.5)
        call_kwargs = mock_scan_cost.call_args
        assert call_kwargs.kwargs.get('fraction') == 0.5 or 0.5 in call_kwargs.args


# --- run() ---------------------------------------------------------------------

class TestRun:

    def _base_args(self):
        return {
            'splits': '4',
            'sample_fraction': '1.0',
            'table': 'table1',
            'table2': 'table2',
            'format': 'keys',
            's3': None,
            'JOB_RUN_ID': 'job-1',
            's3-bucket-name': 'bucket',
        }

    def _setup_run_mocks(self, monkeypatch):
        monkeypatch.setattr(diff_module, 'print_dynamodb_table_info', MagicMock(return_value=(0.10, 1000)))

        client_mock = MagicMock()
        client_mock.describe_table.side_effect = [
            {'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}},
            {'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}},
        ]
        monkeypatch.setattr(diff_module, 'boto3', MagicMock(
            client=MagicMock(return_value=client_mock)
        ))

        monkeypatch.setattr(diff_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(diff_module, 'RateLimiterAggregator', MagicMock())
        monkeypatch.setattr(diff_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

        return client_mock

    def test_no_diffs_prints_no_differences(self, monkeypatch, capsys):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[], [], [], []]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        assert 'No differences found' in out

    def test_diffs_printed_up_to_limit(self, monkeypatch, capsys):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        diffs = [f'- item{i}' for i in range(50)]
        rdd.map.return_value.collect.return_value = [diffs]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        assert '50 differences' in out

    def test_diffs_over_limit_shows_truncation(self, monkeypatch, capsys):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        diffs = [f'- item{i}' for i in range(150)]
        rdd.map.return_value.collect.return_value = [diffs]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        assert 'output truncated' in out
        assert '150 differences' in out
        assert f'first {diff_module.PRINT_LIMIT}' in out

    def test_s3_mode_prints_s3_path(self, monkeypatch, capsys):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()
        args['s3'] = True

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [5, 3, 0, 2]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        assert '10 differences' in out
        assert 's3://bucket/job-1/' in out

    def test_s3_mode_no_diffs(self, monkeypatch, capsys):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()
        args['s3'] = True

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [0, 0, 0, 0]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        assert 'No differences found' in out

    def test_sample_fraction_reduces_segments(self, monkeypatch, capsys):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()
        args['splits'] = '100'
        args['sample_fraction'] = '0.1'

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[] for _ in range(10)]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)

        call_args = spark_context.parallelize.call_args
        segment_list = call_args.args[0]
        assert len(segment_list) == 10
        out = capsys.readouterr().out
        assert 'Sampling' in out
        assert '10 of 100' in out

    def test_parallelize_exception_raised(self, monkeypatch):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()

        spark_context = MagicMock()
        spark_context.parallelize.side_effect = RuntimeError("spark error")

        with pytest.raises(Exception, match="Error in parallel execution"):
            diff_module.run(MagicMock(), spark_context, MagicMock(), args)

    def test_map_collect_exception_raised(self, monkeypatch):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.side_effect = RuntimeError("worker error")

        with pytest.raises(Exception, match="Error in parallel execution"):
            diff_module.run(MagicMock(), spark_context, MagicMock(), args)

    def test_rate_limiter_aggregator_shutdown_on_success(self, monkeypatch):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()

        agg = MagicMock()
        monkeypatch.setattr(diff_module, 'RateLimiterAggregator', MagicMock(return_value=agg))

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[]]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        agg.shutdown.assert_called_once()

    def test_rate_limiter_aggregator_shutdown_on_failure(self, monkeypatch):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()

        agg = MagicMock()
        monkeypatch.setattr(diff_module, 'RateLimiterAggregator', MagicMock(return_value=agg))

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.side_effect = RuntimeError("fail")

        with pytest.raises(Exception):
            diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        agg.shutdown.assert_called_once()

    def test_schema_broadcast_created(self, monkeypatch):
        client_mock = self._setup_run_mocks(monkeypatch)
        client_mock.describe_table.side_effect = [
            {'Table': {'KeySchema': [
                {'AttributeName': 'id', 'KeyType': 'HASH'},
                {'AttributeName': 'ts', 'KeyType': 'RANGE'},
            ]}},
            {'Table': {'KeySchema': [
                {'AttributeName': 'id', 'KeyType': 'HASH'},
            ]}},
        ]
        args = self._base_args()

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[]]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)

        spark_context.broadcast.assert_called_once_with({
            'table1': {'pk': 'id', 'sk': 'ts'},
            'table2': {'pk': 'id', 'sk': None}
        })

    def test_total_cost_printed(self, monkeypatch, capsys):
        monkeypatch.setattr(diff_module, 'print_dynamodb_table_info', MagicMock(return_value=(1.25, 1000)))

        client_mock = MagicMock()
        client_mock.describe_table.return_value = {
            'Table': {'KeySchema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}]}
        }
        monkeypatch.setattr(diff_module, 'boto3', MagicMock(
            client=MagicMock(return_value=client_mock)
        ))
        monkeypatch.setattr(diff_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(diff_module, 'RateLimiterAggregator', MagicMock())
        monkeypatch.setattr(diff_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

        args = self._base_args()
        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[]]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        assert '$2.50' in out

    def test_format_full_passes_false_concise(self, monkeypatch):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()
        args['format'] = 'full'

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[]]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)

    def test_default_splits_is_400(self, monkeypatch, capsys):
        self._setup_run_mocks(monkeypatch)
        args = self._base_args()
        del args['splits']

        spark_context = MagicMock()
        rdd = MagicMock()
        spark_context.parallelize.return_value = rdd
        rdd.map.return_value.collect.return_value = [[] for _ in range(400)]

        diff_module.run(MagicMock(), spark_context, MagicMock(), args)

        call_args = spark_context.parallelize.call_args
        segment_list = call_args.args[0]
        assert len(segment_list) == 400


# --- diff_segment with pk alignment -------------------------------------------

class TestDiffSegmentAlignment:
    """Test the pk-alignment branch (pks diverge, need to scan ahead)."""

    def _make_schema_broadcast(self, pk='pk', sk=None):
        broadcast = MagicMock()
        broadcast.value = {
            'table1': {'pk': pk, 'sk': sk},
            'table2': {'pk': pk, 'sk': sk}
        }
        return broadcast

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_divergent_pks_align_and_report(self, mock_stream_cls, mock_rl):
        """When pks diverge, the algorithm peeks ahead to align."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        # stream_a has: a, c, d
        # stream_b has: b, c, d
        # Expected: '-a', '+b', then c and d match
        items_a = [
            {'pk': {'S': 'a'}, 'val': {'N': '1'}},
            {'pk': {'S': 'c'}, 'val': {'N': '3'}},
            {'pk': {'S': 'd'}, 'val': {'N': '4'}},
        ]
        items_b = [
            {'pk': {'S': 'b'}, 'val': {'N': '2'}},
            {'pk': {'S': 'c'}, 'val': {'N': '3'}},
            {'pk': {'S': 'd'}, 'val': {'N': '4'}},
        ]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: items_a[idx_a[0]]['pk']['S'] if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: None
        stream_a.head_key = lambda: {'pk': items_a[idx_a[0]]['pk']} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = False
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = None
        stream_a.peek = lambda n=0: items_a[idx_a[0] + n] if (idx_a[0] + n) < len(items_a) else None
        stream_a.peek_pk = lambda n: items_a[idx_a[0] + n]['pk']['S'] if (idx_a[0] + n) < len(items_a) else None

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: items_b[idx_b[0]]['pk']['S'] if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: None
        stream_b.head_key = lambda: {'pk': items_b[idx_b[0]]['pk']} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = False
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = None
        stream_b.peek = lambda n=0: items_b[idx_b[0] + n] if (idx_b[0] + n) < len(items_b) else None
        stream_b.peek_pk = lambda n: items_b[idx_b[0] + n]['pk']['S'] if (idx_b[0] + n) < len(items_b) else None

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2',
            {}, {},
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), MagicMock()
        )
        minus_lines = [r for r in result if r.startswith('-')]
        plus_lines = [r for r in result if r.startswith('+')]
        assert len(minus_lines) == 1
        assert len(plus_lines) == 1
        assert '"a"' in minus_lines[0]
        assert '"b"' in plus_lines[0]


# --- diff_segment: sort-key edge cases -----------------------------------------

class TestDiffSegmentSortKeyEdges:
    """Cover remaining branches in the sort-key comparison logic."""

    def _make_schema_broadcast(self):
        broadcast = MagicMock()
        broadcast.value = {
            'table1': {'pk': 'pk', 'sk': 'sk'},
            'table2': {'pk': 'pk', 'sk': 'sk'}
        }
        return broadcast

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_sk_mode_changed_values_concise(self, mock_stream_cls, mock_rl):
        """Same pk, same sk, different non-key attrs in concise → '*'."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_a = {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}}
        item_b = {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '99'}}

        items_a = [item_a]
        items_b = [item_b]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: '1' if idx_a[0] < len(items_a) else None
        stream_a.head_key = lambda: {'pk': {'S': 'a'}, 'sk': {'S': '1'}} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = True
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = 'sk'

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: '1' if idx_b[0] < len(items_b) else None
        stream_b.head_key = lambda: {'pk': {'S': 'a'}, 'sk': {'S': '1'}} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = True
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = 'sk'

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2', {}, {},
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), MagicMock()
        )
        assert len(result) == 1
        assert result[0].startswith('*')

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_sk_mode_changed_values_full(self, mock_stream_cls, mock_rl):
        """Same pk, same sk, different non-key attrs in full → '-' then '+'."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        item_a = {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}}
        item_b = {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '99'}}

        items_a = [item_a]
        items_b = [item_b]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: '1' if idx_a[0] < len(items_a) else None
        stream_a.head_key = lambda: {'pk': {'S': 'a'}, 'sk': {'S': '1'}} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = True
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = 'sk'

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: '1' if idx_b[0] < len(items_b) else None
        stream_b.head_key = lambda: {'pk': {'S': 'a'}, 'sk': {'S': '1'}} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = True
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = 'sk'

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2', {}, {},
            0, 1, False, False, 'job1', False, None,
            self._make_schema_broadcast(), MagicMock()
        )
        assert len(result) == 2
        assert result[0].startswith('-')
        assert result[1].startswith('+')

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_sk_b_greater_than_sk_a(self, mock_stream_cls, mock_rl):
        """sk_a < sk_b → '-' for stream_a item (extra in A at that sk)."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        # A has sk '1','3'; B has sk '2','3'
        # sk '1' < '2' → '-' for A's item; then '2' is only in B → '+'; then '3' matches
        items_a = [
            {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}},
            {'pk': {'S': 'a'}, 'sk': {'S': '3'}, 'val': {'N': '30'}},
        ]
        items_b = [
            {'pk': {'S': 'a'}, 'sk': {'S': '2'}, 'val': {'N': '20'}},
            {'pk': {'S': 'a'}, 'sk': {'S': '3'}, 'val': {'N': '30'}},
        ]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: items_a[idx_a[0]]['sk']['S'] if idx_a[0] < len(items_a) else None
        stream_a.head_key = lambda: {'pk': items_a[idx_a[0]]['pk'], 'sk': items_a[idx_a[0]]['sk']} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = True
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = 'sk'

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: items_b[idx_b[0]]['sk']['S'] if idx_b[0] < len(items_b) else None
        stream_b.head_key = lambda: {'pk': items_b[idx_b[0]]['pk'], 'sk': items_b[idx_b[0]]['sk']} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = True
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = 'sk'

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2', {}, {},
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), MagicMock()
        )
        minus_lines = [r for r in result if r.startswith('-')]
        plus_lines = [r for r in result if r.startswith('+')]
        assert len(minus_lines) >= 1
        assert len(plus_lines) >= 1

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_sk_a_exhausted_before_b(self, mock_stream_cls, mock_rl):
        """A has fewer items for same pk (sk_a becomes None) → remaining B items are '+'."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        # A has pk 'a', sk '1' only; B has pk 'a', sk '1','2','3'
        items_a = [
            {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}},
        ]
        items_b = [
            {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}},
            {'pk': {'S': 'a'}, 'sk': {'S': '2'}, 'val': {'N': '20'}},
            {'pk': {'S': 'a'}, 'sk': {'S': '3'}, 'val': {'N': '30'}},
        ]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: items_a[idx_a[0]]['sk']['S'] if idx_a[0] < len(items_a) else None
        stream_a.head_key = lambda: {'pk': items_a[idx_a[0]]['pk'], 'sk': items_a[idx_a[0]]['sk']} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = True
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = 'sk'

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: items_b[idx_b[0]]['sk']['S'] if idx_b[0] < len(items_b) else None
        stream_b.head_key = lambda: {'pk': items_b[idx_b[0]]['pk'], 'sk': items_b[idx_b[0]]['sk']} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = True
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = 'sk'

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2', {}, {},
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), MagicMock()
        )
        plus_lines = [r for r in result if r.startswith('+')]
        assert len(plus_lines) == 2

    @patch.object(diff_module, 'RateLimiterWorker')
    @patch.object(diff_module, 'SegmentStream')
    def test_sk_b_exhausted_before_a(self, mock_stream_cls, mock_rl):
        """B has fewer items for same pk (sk_b becomes None) → remaining A items are '-'."""
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_session.return_value = MagicMock()
        mock_rl.return_value = mock_rl_instance

        # A has pk 'a', sk '1','2','3'; B has pk 'a', sk '1' only
        items_a = [
            {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}},
            {'pk': {'S': 'a'}, 'sk': {'S': '2'}, 'val': {'N': '20'}},
            {'pk': {'S': 'a'}, 'sk': {'S': '3'}, 'val': {'N': '30'}},
        ]
        items_b = [
            {'pk': {'S': 'a'}, 'sk': {'S': '1'}, 'val': {'N': '10'}},
        ]
        idx_a = [0]
        idx_b = [0]

        stream_a = MagicMock()
        stream_b = MagicMock()

        stream_a.head = lambda: items_a[idx_a[0]] if idx_a[0] < len(items_a) else None
        stream_a.head_pk = lambda: 'a' if idx_a[0] < len(items_a) else None
        stream_a.head_sk = lambda: items_a[idx_a[0]]['sk']['S'] if idx_a[0] < len(items_a) else None
        stream_a.head_key = lambda: {'pk': items_a[idx_a[0]]['pk'], 'sk': items_a[idx_a[0]]['sk']} if idx_a[0] < len(items_a) else None
        stream_a.has_sort_key = True
        stream_a.is_finished = lambda: idx_a[0] >= len(items_a)
        stream_a.advance = lambda: idx_a.__setitem__(0, idx_a[0] + 1)
        stream_a.pk = 'pk'
        stream_a.sk = 'sk'

        stream_b.head = lambda: items_b[idx_b[0]] if idx_b[0] < len(items_b) else None
        stream_b.head_pk = lambda: 'a' if idx_b[0] < len(items_b) else None
        stream_b.head_sk = lambda: items_b[idx_b[0]]['sk']['S'] if idx_b[0] < len(items_b) else None
        stream_b.head_key = lambda: {'pk': items_b[idx_b[0]]['pk'], 'sk': items_b[idx_b[0]]['sk']} if idx_b[0] < len(items_b) else None
        stream_b.has_sort_key = True
        stream_b.is_finished = lambda: idx_b[0] >= len(items_b)
        stream_b.advance = lambda: idx_b.__setitem__(0, idx_b[0] + 1)
        stream_b.pk = 'pk'
        stream_b.sk = 'sk'

        mock_stream_cls.side_effect = [stream_a, stream_b]

        result = diff_module.diff_segment(
            'table1', 'table2', {}, {},
            0, 1, False, True, 'job1', False, None,
            self._make_schema_broadcast(), MagicMock()
        )
        minus_lines = [r for r in result if r.startswith('-')]
        assert len(minus_lines) == 2


# --- Attribute type coverage ---------------------------------------------------

class TestAttributeTypeCoverage:
    """Ensure diff handles all DynamoDB attribute types in comparisons."""

    def test_string_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'S': 'hello'}}
        b = {'pk': {'S': 'k'}, 'attr': {'S': 'hello'}}
        assert diff_module.item_matches(a, b)

    def test_number_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'N': '123.45'}}
        b = {'pk': {'S': 'k'}, 'attr': {'N': '123.45'}}
        assert diff_module.item_matches(a, b)

    def test_binary_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'B': b'\x00\xff'}}
        b = {'pk': {'S': 'k'}, 'attr': {'B': b'\x00\xff'}}
        assert diff_module.item_matches(a, b)

    def test_bool_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'BOOL': True}}
        b = {'pk': {'S': 'k'}, 'attr': {'BOOL': True}}
        assert diff_module.item_matches(a, b)

    def test_null_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'NULL': True}}
        b = {'pk': {'S': 'k'}, 'attr': {'NULL': True}}
        assert diff_module.item_matches(a, b)

    def test_list_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'L': [{'S': 'a'}, {'N': '1'}]}}
        b = {'pk': {'S': 'k'}, 'attr': {'L': [{'S': 'a'}, {'N': '1'}]}}
        assert diff_module.item_matches(a, b)

    def test_map_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'M': {'nested': {'S': 'val'}}}}
        b = {'pk': {'S': 'k'}, 'attr': {'M': {'nested': {'S': 'val'}}}}
        assert diff_module.item_matches(a, b)

    def test_string_set_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'SS': ['a', 'b', 'c']}}
        b = {'pk': {'S': 'k'}, 'attr': {'SS': ['a', 'b', 'c']}}
        assert diff_module.item_matches(a, b)

    def test_number_set_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'NS': ['1', '2', '3']}}
        b = {'pk': {'S': 'k'}, 'attr': {'NS': ['1', '2', '3']}}
        assert diff_module.item_matches(a, b)

    def test_binary_set_type(self):
        a = {'pk': {'S': 'k'}, 'attr': {'BS': [b'\x01', b'\x02']}}
        b = {'pk': {'S': 'k'}, 'attr': {'BS': [b'\x01', b'\x02']}}
        assert diff_module.item_matches(a, b)

    def test_different_string_set(self):
        a = {'pk': {'S': 'k'}, 'attr': {'SS': ['a', 'b']}}
        b = {'pk': {'S': 'k'}, 'attr': {'SS': ['a', 'c']}}
        assert not diff_module.item_matches(a, b)

    def test_different_list(self):
        a = {'pk': {'S': 'k'}, 'attr': {'L': [{'S': 'a'}]}}
        b = {'pk': {'S': 'k'}, 'attr': {'L': [{'S': 'b'}]}}
        assert not diff_module.item_matches(a, b)

    def test_different_map(self):
        a = {'pk': {'S': 'k'}, 'attr': {'M': {'k': {'S': 'v1'}}}}
        b = {'pk': {'S': 'k'}, 'attr': {'M': {'k': {'S': 'v2'}}}}
        assert not diff_module.item_matches(a, b)

    def test_different_bool(self):
        a = {'pk': {'S': 'k'}, 'attr': {'BOOL': True}}
        b = {'pk': {'S': 'k'}, 'attr': {'BOOL': False}}
        assert not diff_module.item_matches(a, b)
