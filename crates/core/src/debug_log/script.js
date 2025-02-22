function toggle_collapsable(
    elements_tag, collapsed_elements_tag, display_style = "flex")
{
    const elements =
        document.querySelectorAll(`[collapse_tag="[${elements_tag}]"]`);
    const collapsed_elements = document.querySelectorAll(
        `[collapse_tag="[${collapsed_elements_tag}]"]`);
    if (elements[0].style.display == "none") {
        for (const elem of elements) {
            elem.style.display = display_style;
        }
        for (const collapsed_elem of collapsed_elements) {
            collapsed_elem.style.display = "none";
        }
    }
    else {
        for (const elem of elements) {
            elem.style.display = "none";
        }
        for (var i = 0; i < collapsed_elements.length; i++) {
            const elem = elements[i];
            const collapsed_elem = collapsed_elements[i];
            gather_nested_highlight_tags(elem, collapsed_elem);
            collapsed_elem.style.display = "flex";
        }
    }
}

function add_tags(tag_set, tag_str)
{
    for (const tag of (tag_str ?? "").split(/\s+/)) {
        tag_set.add(tag);
    }
}

function gather_nested_highlight_tags(element, collapsed_element)
{
    if (collapsed_element.hasAttribute("nested_highlight_tags_gathered")) {
        return;
    }
    let highlight_tag_set = new Set();

    add_tags(
        highlight_tag_set, collapsed_element.getAttribute("highlight_tag"));

    for (elem of element.querySelectorAll(`[highlight_tag]`)) {
        add_tags(highlight_tag_set, elem.getAttribute("highlight_tag"));
    }

    let final_highlight_tag = "";
    for (const tag of highlight_tag_set) {
        final_highlight_tag += tag;
    }

    collapsed_element.setAttribute("highlight_tag", final_highlight_tag.trim());
    collapsed_element.setAttribute("nested_highlight_tags_gathered", "");
}

function highlight(tag)
{
    const highlightables =
        document.querySelectorAll(`[highlight_tag*="[${tag}]"]`);
    for (const elem of highlightables) {
        elem.classList.add('highlighted')
    }
}

function unhighlight(tag)
{
    const highlightables =
        document.querySelectorAll(`[highlight_tag*="[${tag}]"]`);
    for (const elem of highlightables) {
        elem.classList.remove('highlighted')
    }
}
