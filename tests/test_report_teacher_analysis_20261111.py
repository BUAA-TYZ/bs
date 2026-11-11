import pytest

from analysis.report_teacher_analysis_20261111 import metrics,aggregate
from analysis.verify_teacher_analysis_20261111 import independent_metrics


def test_failed_and_late_deliveries_have_capped_cost_not_zero():
    values=[10,60,120,600,700,None]
    actual=metrics(values,True);check=independent_metrics(values,True)
    assert actual['P60']==2/6 and actual['P120']==3/6 and actual['P600']==4/6
    assert actual['R600']==1990/6 and actual['unfinished']==1
    assert all(actual[k]==v for k,v in check.items())


def test_reference_missing_quote_not_actual_unfinished():
    result=metrics([10,None],False)
    assert result['unfinished'] is None and result['unquoted_or_no_path']==1
    assert result['R600']==305 and result['P600']==.5


def test_day_equal_not_image_pooled_and_count_total():
    result=aggregate([metrics([10,None],True),metrics([10]*4,True)])
    assert result['P60']==.75 and result['unfinished']==1
    assert result['R600']==157.5


def test_empty_score_population_rejected():
    with pytest.raises(AssertionError):metrics([],True)
