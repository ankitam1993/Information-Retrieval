from collections import defaultdict
from struct import pack, unpack


def Delta_encoding(postings):

    delta_encoded = dict()

    for key, value in postings.items():
        delta_encoded[key] = []

        for i, val in enumerate(value):

            temp = []

            # since it will be the 1st posting, so Delta_encoding should be only on the list of positions
            if i == 0:
                temp.append(val[0])
                temp.append(val[1])

            else:

                temp.append(val[0]- prev_doc_no)
                temp.append(val[1])

            # Here , since it is a list of positions, delta encoding will be done after the 1st index
            temp.append([val[2][0]])

            for k in range(1, len(val[2])):
               temp[-1].extend([val[2][k] - val[2][k - 1]])

            # Assign the 1st value as the previous value for the next posting
            prev_doc_no = val[0]
            delta_encoded[key].append(temp)


    return delta_encoded


def Delta_decoding(li):

    cur = 0
    delta_decoded_list = []
    type = 'doc'

    while(cur < len(li)):

        if cur == 0:
            delta_decoded_list.append(li[cur])
            prev_doc = li[cur]
            type = 'count'

        elif type == 'doc':
            delta_decoded_list.append(prev_doc + li[cur])
            prev_doc += li[cur]
            type = 'count'


        elif type == 'count':
            delta_decoded_list.append(li[cur])
            count = li[cur]
            type = 'pos'

        elif type == 'pos':

            if count == 1:
                delta_decoded_list.append(li[cur])
            else:
                val = li[cur]
                delta_decoded_list.append(val)

                while(count > 1):
                    cur += 1
                    delta_decoded_list.append(val + li[cur])
                    val += li[cur]
                    count -= 1

            type = 'doc'


        cur += 1


    return delta_decoded_list

def VByte_encoding(delta_encoded):

        bytes_list = []

        for number in delta_encoded:
            bytes_list.extend(encode_number(number))
        return bytes_list

def encode_number(num):

    bytes = []

    while True:
        bytes.insert(0, num % 128)
        if num < 128:
            break
        num = num // 128
    bytes[-1] += 128

    return bytes


def VByte_decoding(bytestream):

        n = 0
        numbers = []

        for byte in bytestream:
            if byte < 128:
                n = 128 * n + byte
            else:
                n = 128 * n + (byte - 128)
                numbers.append(n)
                n = 0
        return numbers
