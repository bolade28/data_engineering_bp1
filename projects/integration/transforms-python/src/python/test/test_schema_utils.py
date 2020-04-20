from unittest.mock import Mock
from python.pnl.schema.schema_dl080_pnl_dmy_by_deal import typed_output_schema as dl080_typed_output_schema
from python.pnl.schema.schema_dl800_new_amended_cancelled_deals import typed_output_schema as dl800_typed_output_schema
from python.exposure.schema_exposure.schema_tr200_exposure_combined_detail import typed_output_schema as tr200_schema
from python.util.schema_utils import get_typed_output_schema, is_multiline


def test_get_typed_output_schema():
    transform_input = Mock()
    transform_input.path = '/BP/IST-IG-SS-Systems/data/raw/endur/dl080-pnl_dmy_by_deal/dl080-pnl_dmy_by_deal'

    actual = get_typed_output_schema(transform_input)

    assert(actual == dl080_typed_output_schema)

    transform_input.path = \
        '/BP/IST-IG-SS-Systems/data/raw/endur/dl800-new_amended_cancelled_deals/dl800-new_amended_cancelled_deals'

    actual = get_typed_output_schema(transform_input)

    assert(actual == dl800_typed_output_schema)

    transform_input.path = \
        '/BP/IST-IG-SS-Systems/data/raw/titan/tr200_exposure/tr200_exposure'

    actual = get_typed_output_schema(transform_input)

    assert(actual == tr200_schema)


def test_is_multiline():
    transform_input = Mock()
    transform_input.path = '/BP/IST-IG-SS-Systems/data/raw/endur/dl080-pnl_dmy_by_deal/dl080-pnl_dmy_by_deal'

    actual = is_multiline(transform_input)

    assert(not actual)
