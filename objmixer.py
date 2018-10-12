#!/usr/bin/env python3

import argparse, sys, re, enum, io
from typing import BinaryIO, Tuple, List
from math import cos, sin, pi

class RecordType(enum.Enum):
    position, normal, texcoord, triangle = range(4)

class VertexObject(object):
    def __init__(self):
        self.position:tuple[float, float, float] = (0, 0, 0)
        self.normal:tuple[float, float, float] = (0, 0, 0)
        self.texcoord:tuple[float, float] = (0, 0)
        self.index:int = 0

class MeshObject(object):
    def __init__(self, file_path:str):
        self.triangles:list[tuple[VertexObject, VertexObject, VertexObject]] = []
        with open(file_path, mode='rb') as fp:
            self.__load(buffer=fp)

    def __load(self, buffer:BinaryIO):
        value, record = b'', []
        value_type:RecordType = RecordType.position
        prev = b''
        vertex:VertexObject = None
        vertex_idx:int = 0
        vertex_map:dict[int, VertexObject] = {}
        while True:
            char = buffer.read(1)
            if not char: break
            if char == b'v': pass
            elif char == b'n':
                if prev == b'v': value_type = RecordType.normal
            elif char == b't':
                if prev == b'v': value_type = RecordType.texcoord
            elif char == b'f':
                if prev == b'\n':value_type = RecordType.triangle
            elif char in b' \n':
                if prev == b'v':
                    vertex_idx += 1
                    value_type = RecordType.position
                    vertex = VertexObject()
                    vertex.index = vertex_idx
                    vertex_map[vertex.index] = vertex
                elif prev == b' ':pass
                else:
                    if value:
                        if value_type == RecordType.triangle:
                            record.append(int(value.split(b'/')[0]))
                        else:
                            record.append(float(value))
                value = b''
                if char == b'\n':
                    if value_type == RecordType.normal:
                        vertex.normal = tuple(record)
                    elif value_type == RecordType.position:
                        vertex.position = tuple(record)
                    elif value_type == RecordType.texcoord:
                        if len(record) == 2: vertex.texcoord = tuple(record)
                    elif value_type == RecordType.triangle:
                        triangle = tuple(vertex_map.get(x) for x in record) # type:tuple[VertexObject,VertexObject,VertexObject]
                        if triangle: self.triangles.append(triangle)
                    record = []
            elif char == b'#':
                while True:
                    char = buffer.read(1)
                    if not char or char == b'\n': break
            else:
                value += char
            prev = char
        print('# vertices:{} triangles:{}'.format(len(vertex_map), len(self.triangles)))

    def __encode_tuple(self, buffer:io.StringIO, data:Tuple[float, ...]):
        for value in data: buffer.write(' {:.7f}'.format(value))

    def __get_unique_vertices(self):
        hash_map = {}
        vertex_array: list[VertexObject] = []
        for triangle in self.triangles:
            for vertex in triangle:
                uuid = vertex.__hash__()
                if uuid not in hash_map:
                    vertex_array.append(vertex)
                    vertex.index = len(vertex_array)
                    hash_map[uuid] = vertex
        return vertex_array

    def __rotate_x(self, angle:float):
        if angle == 0: return
        angle = pi / 180 * angle
        matrix = (
            1,          0,           0,
            0, cos(angle), -sin(angle),
            0, sin(angle),  cos(angle)
        )
        self.__rotate_with_matrix(matrix)

    def __rotate_y(self, angle:float):
        if angle == 0: return
        angle = pi / 180 * angle
        matrix = (
             cos(angle), 0, sin(angle),
                      0, 1,          0,
            -sin(angle), 0, cos(angle)
        )
        self.__rotate_with_matrix(matrix)

    def __rotate_z(self, angle:float):
        if angle == 0: return
        angle = pi / 180 * angle
        matrix = (
            cos(angle), -sin(angle), 0,
            sin(angle),  cos(angle), 0,
                     0,           0, 1
        )
        self.__rotate_with_matrix(matrix)

    def __vector_dot(self, row:Tuple[float, float, float], col:Tuple[float, float, float]):
        return row[0]*col[0] + row[1]*col[1] + row[2]*col[2]

    def __rotate_with_matrix(self, matrix:Tuple[float, ...]):
        vertex_array = self.__get_unique_vertices()
        for vertex in vertex_array:
            position = vertex.position
            vertex.position = (
                self.__vector_dot(matrix[0:3], position),
                self.__vector_dot(matrix[3:6], position),
                self.__vector_dot(matrix[6:9], position)
            )
            normal = vertex.normal
            vertex.normal = (
                self.__vector_dot(matrix[0:3], normal),
                self.__vector_dot(matrix[3:6], normal),
                self.__vector_dot(matrix[6:9], normal)
            )

    def rotate(self, angles:Tuple[float, float, float]):
        self.__rotate_x(angles[0])
        self.__rotate_y(angles[1])
        self.__rotate_z(angles[2])

    def align(self):
        span_x = [1000000,-1000000]
        span_y = [1000000,-1000000]
        span_z = [1000000,-1000000]
        vertex_array = self.__get_unique_vertices()
        for vertex in vertex_array:
            p = vertex.position
            span_x[0] = min(span_x[0], p[0])
            span_x[1] = max(span_x[1], p[0])
            span_y[0] = min(span_y[0], p[1])
            span_y[1] = max(span_y[1], p[1])
            span_z[0] = min(span_z[0], p[2])
            span_z[1] = max(span_z[1], p[2])
        anchor = ((span_x[0] + span_x[1])/2, span_y[0], (span_z[0] + span_z[1])/2)
        print('#', span_x, span_y,span_z)
        print('#', anchor)
        for vertex in vertex_array:
            p = vertex.position
            vertex.position = (
                p[0] - anchor[0],
                p[1] - anchor[1],
                p[2] - anchor[2]
            )

    def dump(self):
        buffer:io.StringIO = io.StringIO()
        vertex_array:list[VertexObject] = self.__get_unique_vertices()
        for vertex in vertex_array:
            buffer.write('v')
            self.__encode_tuple(buffer, vertex.position)
            buffer.write('\n')
            if vertex.normal:
                buffer.write('vn')
                self.__encode_tuple(buffer, vertex.normal)
                buffer.write('\n')
            if vertex.texcoord:
                buffer.write('vt')
                self.__encode_tuple(buffer, vertex.texcoord)
                buffer.write('\n')
        buffer.write('# {} verticies\n'.format(len(vertex_array)))
        for triangle in self.triangles:
            buffer.write('f')
            for vertex in triangle: buffer.write(' {}/{}/{}'.format(vertex.index, vertex.index, vertex.index))
            buffer.write('\n')
        buffer.write('# {} elements\n'.format(len(self.triangles)))
        buffer.seek(0)
        return buffer.read()

    def append(self, target: 'MeshObject'):
        for triangle in target.triangles:
            self.triangles.append(triangle)


if __name__ == '__main__':
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--obj-file', '-f', nargs='+', required=True)
    arguments.add_argument('--rotate-x', '-x', type=float, default=90)
    arguments.add_argument('--rotate-y', '-y', type=float, default=0)
    arguments.add_argument('--rotate-z', '-z', type=float, default=-90)
    arguments.add_argument('--align', '-a', action='store_true', default=False)
    options = arguments.parse_args(sys.argv[1:])
    mesh:MeshObject = None
    for item_path in options.obj_file:
        item = MeshObject(file_path=item_path)
        if not mesh:
            mesh = item
            continue
        mesh.append(item)
    mesh.rotate(angles=(options.rotate_x,options.rotate_y,options.rotate_z))
    if options.align: mesh.align()
    print(mesh.dump())
