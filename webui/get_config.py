import socket, re

def get_config(host, port):
    config = {}
    #Summary
    summary_str = ''
    summary = {}
    meta_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    meta_sock.connect((host, port))
    meta_sock.send("PING\r\nVersion: KFS/1.0\r\nCseq: 1\r\nClient-Protocol-Version: 111\r\n\r\nDISCONNECT\r\n\r\n")
    meta_sock.recv(5)
    data = meta_sock.recv(65536)
    while len(data):
        summary_str = summary_str + data
        data = meta_sock.recv(65536)
    meta_sock.close()
    for line in summary_str.split("\r\n"):
        try:
            [ item, data ] = line.split(': ')
            if re.search('\t', data):
                pairs = data.split('\t')
                if re.search('= ', data):
                    dict = {}
                    for pair in pairs:
                        [ key, value ] = pair.split('= ')
                        dict[key] = value
                    summary[item] = dict
                else:
                    for pair in pairs:
                        dict = {}
                        for spair in pair.split(', '):
                            [ key, value ] = spair.split('=')
                            dict[key] = value
                        if not summary.has_key(item): summary[item] = {}
                        summary[item][dict['s']] = dict
            else:
                summary[item] = data
        except ValueError:
            continue
    for pair in summary['Config'].split(';'):
        try:
            [ key, value ] = pair.split('=')
            config[key] = value
        except ValueError:
            continue
    return config
