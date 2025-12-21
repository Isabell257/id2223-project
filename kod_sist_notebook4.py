#Go through all bath locations and write their html-files
for location_dict in bath_locations:
    bath_location = location_dict["bath_location"]
    bath_location_norm =bath_location.lower()
    bath_location_norm =bath_location_norm.replace(' ', '_')
    
    #Change to root directory
    os.chdir(root_dir)

    html_file = f"docs/water_temp/{bath_location_norm}.html"
    folder = f"docs/water_temp/water_temp_{bath_location_norm}"
    rel_folder = f"water_temp_{bath_location_norm}"
    files = [f for f in os.listdir(folder) if f != ".gitkeep"]

    #Checking whether there are images in the folder and build html
    with open(html_file, "w") as fp:
       
        #Adding dropdown
        fp.write("---\nlayout: default\n---\n")
        fp.write("{% include dropdown.html %}\n")

         #Writing a page title
        fp.write(f"<h2>{bath_location}</h2>\n")

        #If folder is empty, sensor is not running
        if not files:
            fp.write(f"<p>No predictions for this week for {bath_location}.</p>\n")
        #Else we show the images
        else:
            fp.write(f"<h3>Water temperature predictions and hindsight graph for {bath_location}</h3>\n")
            for f in files:
                if f.lower().endswith((".png", ".jpg", ".jpeg")):
                    fp.write(
                        f'<img src="{rel_folder}/{f}" style="max-width:600px;"><br>\n'
                    )