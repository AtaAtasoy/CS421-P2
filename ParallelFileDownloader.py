import socket
import sys
import threading
import math

# Each thread executes this code
def downloader(start, end, url, filename):
    parsed_url = address_parser(url)
    path = parsed_url['path']
    host = parsed_url['host']

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.connect((host, 80))
    request = f'GET {path} HTTP/1.1\r\nHost:{host}\r\nRange: bytes={start}-{end}\r\n\r\n'
    tcp_socket.sendall(bytes(request, encoding="utf-8"))
    result = '\n'.join(str(tcp_socket.recv(4096), encoding='utf-8').split('\n')[10:])
    
    tcp_socket.close()
    with open(filename, "r+b") as fp:
        fp.seek(start)
        var = fp.tell()
        fp.write(bytes(result, encoding='utf-8'))


def address_parser(url: str):
    url = url.replace("http://", "")
    url = url.replace("https://", "")

    host = url[0: url.index("/")]
    path = url[url.index("/"):]

    return {'host': host, 'path': path}

def handle_index_file(index_url) -> list:
    file_urls = list()
    parsed_url = address_parser(index_url)
    path = parsed_url['path']
    host = parsed_url['host']

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.connect((host, 80))
    request = f'GET {path} HTTP/1.1\r\nHost:{host}\r\n\r\n'
    tcp_socket.sendall(bytes(request, encoding="utf-8"))
    

    result = str(tcp_socket.recv(4096), 'utf-8')
    status = result[:result.index('\n')]
    if '200' not in status and '301' not in status:
        print(f'Index file not found')
    else:
        print('Index file is downloaded')
        file_urls = [x for x in result.split() if 'www.' in x]
        print(f'There are {len(file_urls)} files in the index')
        tcp_socket.close()
        return file_urls

def handle_downloads(file_urls, connection_count):
    file_count = 0
    for url in file_urls:
        file_parts = []
        parsed_url = address_parser(url)
        path, host = parsed_url['path'], parsed_url['host']
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        tcp_socket.connect((host, 80))
        request = f'HEAD {path} HTTP/1.1\r\nHost:{host}\r\n\r\n'
        tcp_socket.sendall(bytes(request, encoding="utf-8"))

        # Send the HEAD request to check if file exists
        result = str(tcp_socket.recv(4096), 'utf-8')
        status = result[:result.index('\n')]
        if '200' not in status and '301' not in status:
            file_count += 1
            print(f'{file_count} {url} not found.')
        else:
            file_count += 1
            file_name = path.split('/')[-1]
            for line in result.split('\n'):
                if "Content-Length" in line:
                    content_length = int(line[line.index(':') + 1:])
                    fp = open(file_name, "wb")
                    fp.write(bytes('\0' * content_length, encoding='utf-8'))
                    fp.close()
                    
                    if content_length % connection_count == 0:
                        part = int(content_length / connection_count)
                        for i in range(connection_count):
                            start = part * i
                            end = start + part
                            file_parts.append((start, end))
                            args = {'start': start, 'end': end, 'url': url, 'filename': file_name}
                            t = threading.Thread(target=downloader, kwargs=args)
                            t.setDaemon(True)
                            t.start()
                    else:
                        connection_threshold = 0
                        for i in range(connection_count):
                            if (connection_threshold < content_length - math.floor(content_length / connection_count) * connection_count):
                                part = math.floor(content_length / connection_count) + 1
                            else:
                                part = math.floor(content_length / connection_count)
                            start = part * i
                            end = start + part
                            file_parts.append((start, end))
                            args = {'start': start, 'end': end, 'url': url, 'filename': file_name}
                            t = threading.Thread(target=downloader, kwargs=args)
                            t.daemon = True
                            t.start()
                            connection_threshold += 1
                    # Join the threads
                    main_thread = threading.current_thread()
                    for t in threading.enumerate():
                        if t is main_thread:
                            continue
                        t.join()
                    print(f'{file_count}. {file_name} (size = {content_length}) is downloaded')
                    for i in range(0, len(file_parts) - 1):
                        part = file_parts[i]
                        print(f'File parts: {part[0]}:{part[1]}({part[1] - part[0]}), ', end='')
                    last_chunk = file_parts[len(file_parts) - 1]
                    print(f'{last_chunk[0]}:{last_chunk[1]}({last_chunk[1] - last_chunk[0]})')

if __name__ == "__main__":
    cmd_args = sys.argv[1:]
    index_url = cmd_args[0]
    connection_count = int(cmd_args[1])
    print(f'URL of the index file: {index_url}')
    print(f'Number of parallel connections: {connection_count}')
    
    file_urls = handle_index_file(index_url)
    handle_downloads(file_urls, connection_count)
   