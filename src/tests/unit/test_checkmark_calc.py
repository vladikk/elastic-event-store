from .context import ees
from ees.model import CheckmarkCalc

def test_increment_page_item(mocker):
    c = CheckmarkCalc()
    
    (page, page_item) = c.next_page_and_item(0, 0)

    assert page == 0
    assert page_item == 1

def test_increment_page_item2(mocker):
    c = CheckmarkCalc()
    
    (page, page_item) = c.next_page_and_item(100, 10)

    assert page == 100
    assert page_item == 11

def test_increment_page_item_over_page_size(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    (page, page_item) = c.next_page_and_item(0, 99)

    assert page == 1
    assert page_item == 0


def test_increment_page_item_over_page_size2(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    (page, page_item) = c.next_page_and_item(100, 99)

    assert page == 101
    assert page_item == 0


def test_page_item_to_checkmark1(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    checkmark = c.to_checkmark(0, 0)

    assert checkmark == 0

def test_page_item_to_checkmark2(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    checkmark = c.to_checkmark(0, 52)

    assert checkmark == 52

def test_page_item_to_checkmark3(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    checkmark = c.to_checkmark(12, 0)

    assert checkmark == 1200

def test_page_item_to_checkmark4(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    checkmark = c.to_checkmark(12, 52)

    assert checkmark == 1252

def test_checkmark_to_page_item1(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    (p, i) = c.to_page_item(0)

    assert p == 0
    assert i == 0

def test_checkmark_to_page_item2(mocker):
    c = CheckmarkCalc()
    c.page_size = 100

    (p, i) = c.to_page_item(52)

    assert p == 0
    assert i == 52

def test_checkmark_to_page_item3(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    (p, i) = c.to_page_item(1200)

    assert p == 12
    assert i == 0

def test_checkmark_to_page_item4(mocker):
    c = CheckmarkCalc()
    c.page_size = 100
    
    (p, i) = c.to_page_item(1252)

    assert p == 12
    assert i == 52