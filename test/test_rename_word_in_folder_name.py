from pytest import raises
from utils.renaming_folders import rename_word_in_folder_name


def test_function_returns_the_same_string_when_the_word_is_an_empty_string():

    input_word = ''
    target_word_input = 'hello'
    replacement_word_input = 'world'

    expected_output = ''
    result = rename_word_in_folder_name(
        input_word, target_word_input, replacement_word_input)

    assert result == expected_output


def test_function_returns_the_same_string_when_the_word_does_not_contain_the_target_word():

    input_word = 'coding'
    target_word_input = 'hello'
    replacement_word_input = 'world'

    expected_output = 'coding'
    result = rename_word_in_folder_name(
        input_word, target_word_input, replacement_word_input)

    assert result == expected_output


def test_function_returns_a_string_of_one_word_with_target_word_changed_to_replacement_word():

    input_word = 'hello'
    target_word_input = 'hello'
    replacement_word_input = 'world'

    expected_output = 'world'
    result = rename_word_in_folder_name(
        input_word, target_word_input, replacement_word_input)

    assert result == expected_output


def test_function_returns_a_string_of_only_changing_the_target_word_if_there_are_other_words_in_the_input_word():

    input_word = 'hello everyone'
    target_word_input = 'hello'
    replacement_word_input = 'bye'

    expected_output = 'bye everyone'
    result = rename_word_in_folder_name(
        input_word, target_word_input, replacement_word_input)

    assert result == expected_output


def test_function_returns_a_string_changing_all_occurences_of_the_target_word_if_it_appears_more_than_once():

    input_word = 'hello everyone hello again'
    target_word_input = 'hello'
    replacement_word_input = 'bye'

    expected_output = 'bye everyone bye again'
    result = rename_word_in_folder_name(
        input_word, target_word_input, replacement_word_input)

    assert result == expected_output


def test_function_returns_a_string_changing_the_target_word_even_if_it_has_capital_letters():

    input_word = 'Hello everyone hello again'
    target_word_input = 'Hello'
    replacement_word_input = 'Yo'

    expected_output = 'Yo everyone hello again'
    result = rename_word_in_folder_name(
        input_word, target_word_input, replacement_word_input)

    assert result == expected_output


def test_function_returns_a_string_changing_the_target_word_even_if_it_is_a_string_containg_a_number_or_punctuation():

    input_word_with_number = 'hello1 everyone'
    target_word_input_with_number = 'hello1'
    replacement_word_input_for_number = 'hello'

    expected_output_for_number = 'hello everyone'
    result_for_number = rename_word_in_folder_name(
        input_word_with_number,
        target_word_input_with_number,
        replacement_word_input_for_number)

    assert result_for_number == expected_output_for_number

    input_word_with_punctuation = 'hello! all'
    target_word_input_with_punctuation = 'hello!'
    replacement_word_input_for_punctuation = 'hello'

    expected_output_for_punctuation = 'hello all'
    result_for_punctuation = rename_word_in_folder_name(
        input_word_with_punctuation,
        target_word_input_with_punctuation,
        replacement_word_input_for_punctuation)

    assert result_for_punctuation == expected_output_for_punctuation


def test_function_raises_an_error_if_any_of_the_inputs_are_not_a_string():

    input_word = 'Hello everyone hello again'
    target_word_input = 'Hello'
    replacement_word_input = 'Yo'

    invalid_input_word = 5
    invalid_target_word_input = ['hello']
    invalid_replacement_word_input = {'hello': 'world'}

    with raises(Exception):
        rename_word_in_folder_name(
            invalid_input_word,
            target_word_input,
            replacement_word_input)

    with raises(Exception):
        rename_word_in_folder_name(
            input_word,
            invalid_target_word_input,
            replacement_word_input)

    with raises(Exception):
        rename_word_in_folder_name(
            input_word,
            target_word_input,
            invalid_replacement_word_input)
