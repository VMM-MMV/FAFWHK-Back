# with open(file_path, "a") as file:
#     while True:
#         if "data" in r:
#             retrieved += len(r["data"])
#             print(f"Retrieved {retrieved} papers...")
#             for paper in r["data"]:
#                 # Add a comma if the file is not empty (i.e., it's not the first paper being written)
#                 if file_exists:
#                     file.write(",\n")
#                 else:
#                     file_exists = True  # Now the file is no longer empty
#                 # Write the paper as JSON on the line
#                 print(json.dumps(paper), file=file)
#         if "token" not in r:
#             break
#         r = requests.get(f"{url}&token={r['token']}").json()