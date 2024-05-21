import logging

def get_folder_with_files(target_word, list_of_files):

    logger = logging.getLogger('Utils')

    try:
        untransformed_file_list = [
            file for file in list_of_files if target_word not in file]

        grouped_files = {}

        # Group the files by folder name
        for file_path in untransformed_file_list:
            folder_name = file_path.split('/')[0]
            if folder_name in grouped_files:
                grouped_files[folder_name].append(file_path)
            else:
                grouped_files[folder_name] = [file_path]

        return grouped_files
    
    except Exception as err:
        logger.error(err)
        raise Exception