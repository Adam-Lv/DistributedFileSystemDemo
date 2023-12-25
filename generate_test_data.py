"""
生成指定大小，指定位置的测试文件
"""


def generate_text_file(size_in_mb, file_name='output.txt'):
    # Convert size from MB to bytes
    size_in_bytes = size_in_mb * 1024 * 1024 - 20

    # Create a string of repeating "A-Za-z0-9"
    repeating_str = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    size_str = len(repeating_str)
    repeat_times, last_length = size_in_bytes // size_str, size_in_bytes % size_str

    # Write data to the file in blocks
    with open(file_name, 'w') as f:
        while repeat_times > 20:
            # Write 20 times of repeating_str in one line
            f.write(repeating_str * 20)
            f.write('\n')
            repeat_times -= 20
            f.flush()
        f.write(repeating_str * repeat_times)
        f.write(repeating_str[:last_length])


if __name__ == '__main__':
    generate_text_file(2, './test_files/upload/2MB_test.txt')
