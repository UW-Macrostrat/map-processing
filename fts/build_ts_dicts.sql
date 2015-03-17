CREATE TEXT SEARCH DICTIONARY macro_simple (
    template = simple,
    stopwords = english
);


CREATE TEXT SEARCH CONFIGURATION macro (parser='default');


ALTER TEXT SEARCH CONFIGURATION macro
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
                  word, hword, hword_part
    WITH macro_simple;


CREATE TEXT SEARCH DICTIONARY macro_snowball (
    template = snowball,
    language = english
);


ALTER TEXT SEARCH CONFIGURATION macro
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
                  word, hword, hword_part
    WITH macro_simple, macro_snowball;


ALTER TEXT SEARCH DICTIONARY macro_simple ( accept = false );


CREATE TEXT SEARCH DICTIONARY macro_synonym (
    template = synonym,
    synonyms = macro_synonyms
);


ALTER TEXT SEARCH CONFIGURATION macro
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
                  word, hword, hword_part
    WITH macro_simple, macro_synonym, macro_snowball;



ALTER TEXT SEARCH DICTIONARY macro_synonym (synonyms=macro_synonyms);



CREATE TEXT SEARCH DICTIONARY macro_thesaurus (
    TEMPLATE = thesaurus,
    DICTFILE = macro_thesaurus,
    DICTIONARY = macro_snowball
);


ALTER TEXT SEARCH CONFIGURATION macro
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
                  word, hword, hword_part
    WITH macro_simple, macro_thesaurus, macro_snowball;



