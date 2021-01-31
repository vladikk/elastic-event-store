from .context import ees
from ees.model import CheckpointCalc

def test_increment_page_item(mocker):
    c = CheckpointCalc()
    
    (page, page_item) = c.next_page_and_item(0, 0)

    assert page == 0
    assert page_item == 1

def test_increment_page_item2(mocker):
    c = CheckpointCalc()
    
    (page, page_item) = c.next_page_and_item(100, 10)

    assert page == 100
    assert page_item == 11

def test_increment_page_item_over_page_size(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    (page, page_item) = c.next_page_and_item(0, 99)

    assert page == 1
    assert page_item == 0


def test_increment_page_item_over_page_size2(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    (page, page_item) = c.next_page_and_item(100, 99)

    assert page == 101
    assert page_item == 0


def test_page_item_to_checkpoint1(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    checkpoint = c.to_checkpoint(0, 0)

    assert checkpoint == 0

def test_page_item_to_checkpoint2(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    checkpoint = c.to_checkpoint(0, 52)

    assert checkpoint == 52

def test_page_item_to_checkpoint3(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    checkpoint = c.to_checkpoint(12, 0)

    assert checkpoint == 1200

def test_page_item_to_checkpoint4(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    checkpoint = c.to_checkpoint(12, 52)

    assert checkpoint == 1252

def test_checkpoint_to_page_item1(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    (p, i) = c.to_page_item(0)

    assert p == 0
    assert i == 0

def test_checkpoint_to_page_item2(mocker):
    c = CheckpointCalc()
    c.page_size = 100

    (p, i) = c.to_page_item(52)

    assert p == 0
    assert i == 52

def test_checkpoint_to_page_item3(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    (p, i) = c.to_page_item(1200)

    assert p == 12
    assert i == 0

def test_checkpoint_to_page_item4(mocker):
    c = CheckpointCalc()
    c.page_size = 100
    
    (p, i) = c.to_page_item(1252)

    assert p == 12
    assert i == 52