from lxml import etree


class FitsIndexer:

    def __init__(self, fits_xml):
        try:
            self.fits_xml = fits_xml.encode('utf8')
        except AttributeError:
            self.fits_xml = fits_xml

    def index_data(self):
        data = {}
        try:
            root = etree.fromstring(self.fits_xml)
            image_width = root.xpath('fits_output:metadata/fits_output:image/fits_output:imageWidth',
                namespaces={'fits_output': 'http://hul.harvard.edu/ois/xml/ns/fits/fits_output'})[0]
            image_height = root.xpath('fits_output:metadata/fits_output:image/fits_output:imageHeight',
                namespaces={'fits_output': 'http://hul.harvard.edu/ois/xml/ns/fits/fits_output'})[0]
            data['fits_image_width_lsi'] = image_width.text
            data['fits_image_height_lsi'] = image_height.text
        except IndexError:
            pass
        except etree.XMLSyntaxError as e:
            raise Exception(f'error parsing FITS: {e}\n{self.fits_xml}')
        return data

