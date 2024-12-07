import asyncio
import shutil
import os
import aiofiles.os
from pathlib import Path
import argparse

CHUNK_SIZE = 5
DEFAULT_SCR_FOLDER = './src'
DEFAULT_TARGET_FOLDER = './target_folder'


async def read_folder(folder_name) -> list:
    file_list = []
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(file_path)
    return file_list


async def move_files(file_list, target_folder):
    await create_folder_if_missing(target_folder)
    for file_name in file_list:
        extension = os.path.splitext(file_name)[1].lstrip(".")
        if extension == '':
            extension = 'empty_extension'
        path_to_final_folder = os.path.join(target_folder, extension)
        await create_folder_if_missing(path_to_final_folder)
        await move_file_async(Path(file_name), Path(path_to_final_folder))


async def move_file_async(src, dest):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, shutil.copy, src, dest)


async def create_folder_if_missing(folder_name):
    try:
        await aiofiles.os.makedirs(folder_name, exist_ok=True)
    except FileExistsError:
        pass


async def main(source_folder: str, target_folder: str):
    files = await read_folder(source_folder)
    print(f"Found {len(files)} files to copy.")

    chunks = [files[i:i + CHUNK_SIZE] for i in range(0, len(files), CHUNK_SIZE)]

    tasks = [move_files(chunk, target_folder) for chunk in chunks]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Enter the params")
    parser.add_argument("--src_folder", type=str, help="Source folder")
    parser.add_argument("--target_folder", type=str, help="Target folder")
    args = parser.parse_args()

    src_folder = DEFAULT_SCR_FOLDER if args.src_folder is None else args.src_folder
    target_folder = DEFAULT_TARGET_FOLDER if args.target_folder is None else args.src_folder

    asyncio.run(main(src_folder, target_folder))
