/*
 * Copyright (c) 2009-2011, Travis Parker
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials provided
 *       with the distribution.
 *     * Neither the name of the author nor the names of other
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
A dynamically resizing string struct and related macros

Everything needed for the common operations are in this single
header file, thanks to some significant preprocessor abuse

API
===

offsetstring
    the typedef for the objects. these are allocated on the heap and generally
    only pointers to them are passed around.

void offsetstring_new(
offsetstring *string, char *rc)
    create a new instance and initialize it. if any malloc failed, then *rc
    will be set to something non-zero.

void offsetstring_wrap(
offsetstring *string, char *buffer, int length, char *rc)
    create a new instance and initialize it using the existing buffer and
    length.

void offsetstring_del(
offsetstring *string)
    frees an offsetstring and its associated buffer.

char * offsetstring_data(
offsetstring *string)
    returns the raw buffer associated with a string. usually, offsetstring_head
    is going to be more useful.

unsigned int offsetstring_offset(
offsetstring *string)
    returns the number of bytes *already behind* the cursor

unsigned int offsetstring_capacity(
offsetstring *string)
    returns the total allocated amount in the string's associated buffer

char * offsetstring_head(
offsetstring *string)
    returns a char pointer to the string's buffer *at the cursor*

int offsetstring_remaining(
offsetstring *string)
    returns the number of bytes allocated and still ahead of the cursor

void offsetstring_resize(
offsetstring *string, int distance, char *rc)
    ensures that string has at least distance bytes left ahead of the cursor
    (distance is based on offsetstring_remaining, not offsetstring_capacity).
    if realloc fails, then *rc is set to something non-zero and the string is
    left unmodified. NULL can be passed as rc, but in that case success is
    assumed.

void offsetstring_readahead(
offsetstring *string, char *destination, int distance, int *written)
    reads up to distance bytes from the string (starting from its cursor) into
    destination. *written is set to the number of bytes actually copied (the
    minimum of destination and offsetstring_remaining(string) will be used).

void offsetstring_read(
offsetstring *string, char *destination, int distance, int *written)
    does everything that offsetstring_readahead does, and also moves the cursor
    to the end of the section that was just read.

void offsetstring_writeahead(
offsetstring *string, char *source, int distance, char *rc)
    writes distance bytes from the source buffer into the string. if a realloc
    is needed but fails, *rc is set to something non-zero and the string is
    left unmodified.

void offsetstring_write(
offsetstring *string, char *source, int distance, char *rc)
    does everything that offsetstring_writeahead does, and also moves the
    cursor to the end of the section that was just written (or leaves it in
    place if *rc is non-zero).

void offsetstring_seek(
offsetstring *string, int position)
    move the cursor to a position relative to the beginning of the buffer
*/

#ifndef _HAVE_OFFSETSTRING_H
#define _HAVE_OFFSETSTRING_H

#define OFFSETSTRING_INITIAL_SIZE 0x1000

typedef struct {
    char *data;
    unsigned int offset;
    unsigned int capacity;
} offsetstring;


#define offsetstring_new(string, rc) do {                   \
    (string) = malloc(sizeof(offsetstring));                \
    *(rc) = ((string) == NULL);                             \
    if (!*(rc)) {                                           \
        (string)->data = malloc(OFFSETSTRING_INITIAL_SIZE); \
        *(rc) = ((string)->data == NULL);                   \
        if (!*(rc)) {                                       \
            (string)->offset = 0;                           \
            (string)->capacity = OFFSETSTRING_INITIAL_SIZE; \
        }                                                   \
        else free((string));                                \
    }                                                       \
} while (0)

#define offsetstring_wrap(string, buffer, length, rc) do { \
    (string) = malloc(sizeof(offsetstring));               \
    *(rc) = ((string) == NULL);                            \
    if (!*(rc)) {                                          \
        (string)->data = (buffer);                         \
        (string)->offset = 0;                              \
        (string)->capacity = (length);                     \
    }                                                      \
} while (0)


#define offsetstring_del(string) do { \
    free((string)->data);             \
    free((string));                   \
} while (0)


#define offsetstring_data(string) (string)->data
#define offsetstring_offset(string) (string)->offset
#define offsetstring_capacity(string) (string)->capacity

#define offsetstring_head(string) ((string)->data + (string)->offset)
#define offsetstring_remaining(string) ((string)->capacity - (string)->offset)


#define offsetstring_resize(string, distance, rc) do {                \
    *(rc) = 0;                                                        \
    unsigned int old_len = (string)->capacity;                        \
    while (offsetstring_remaining(string) < distance)                 \
        (string)->capacity *= 2;                                      \
    if ((string)->capacity != old_len) {                              \
        char *temp = (string)->data;                                  \
        (string)->data = realloc((string)->data, (string)->capacity); \
        *(rc) = (temp == NULL);                                       \
        if (*(rc)) (string)->data = temp;                             \
        else (string)->capacity = old_len;                            \
    }                                                                 \
} while (0)


#define offsetstring_readahead(string, destination, distance, written) do { \
    int remaining = offsetstring_remaining(string);                         \
    *(written) = remaining < distance ? remaining : distance;               \
    memcpy(destination, offsetstring_head(string), *(written));             \
} while (0)

#define offsetstring_read(string, destination, distance, written) do { \
    offsetstring_readahead(string, destination, distance, written);    \
    (string)->offset += written;                                       \
} while (0)


#define offsetstring_writeahead(string, source, distance, rc) do { \
    offsetstring_resize(string, distance, rc);                     \
    if (!*(rc))                                                    \
        memcpy(offsetstring_head(string), source, distance);       \
} while (0)

#define offsetstring_write(string, source, distance, rc) do { \
    offsetstring_writeahead(string, source, distance, rc);    \
    if (!*(rc)) (string)->offset += distance;                 \
} while (0)


#define offsetstring_seek(string, position) do { \
    int pos = (position);                        \
    (string)->offset = pos < 0 ?                 \
        0 :                                      \
        (pos > (string)->capacity ?              \
            (string)->capacity :                 \
            pos);                                \
} while (0)


#endif /* _HAVE_OFFSETSTRING_H */
