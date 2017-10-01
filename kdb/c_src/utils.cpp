///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : utils.cpp
/// Author  : Evgeny Khirin <>
/// Description : Unsorted utilities.
///-------------------------------------------------------------------
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "utils.hpp"

//--------------------------------------------------------------------
// parser state
//--------------------------------------------------------------------
enum parser_state {
	look_open_bracket,
	look_first_atom,
	look_atom_start,
	look_atom_end
};

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void parse_atoms_list(const std::string& str, std::list<std::string>& res) {
	parser_state state = look_open_bracket;
	const char* s = str.c_str();
	std::string atom;
	while (*s) {
		char c = *s++;
		switch (state) {
		case look_open_bracket:
			if (c == '[') {
				state = look_first_atom;
			}
			break;
		case look_first_atom:
			if (c == ',') {
				throw list_parse();
			}
			if (c == ']') {
				return;
			}
			if (!isspace(c)) {
				state = look_atom_end;
				atom.clear();
				atom.push_back(c);
			}
			break;
		case look_atom_start:
			if (c == ',' || c == ']') {
				throw list_parse();
			}
			if (!isspace(c)) {
				state = look_atom_end;
				atom.clear();
				atom.push_back(c);
			}
			break;
		case look_atom_end:
			if (c != ',' && c != ']') {
				atom.push_back(c);
			} else {
				res.push_back(atom);
				if (c == ']') {
					return;
				}
				state = look_atom_start;
			}
			break;
		}
	}
	throw list_parse();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
#ifdef _WIN32
void msleep(unsigned miliseconds) {
	Sleep(miliseconds);
}
#else
void msleep(unsigned miliseconds) {
	unsigned seconds = miliseconds / 1000;
	unsigned microseconds = (miliseconds - seconds * 1000) * 1000;
	while ((seconds = sleep(seconds)) != 0) {
	}
	usleep(microseconds);
}
#endif
