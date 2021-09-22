import { getFieldsFromTemplate } from '.';

describe('getFieldsFromTemplate', () => {
	it('returns an empty array when passed null', () => {
		expect(getFieldsFromTemplate(null)).toStrictEqual([]);
	});
	it('returns fields as an array of strings', () => {
		expect(getFieldsFromTemplate('{{ field }}')).toBe(['field']);
	});
	// TODO: Cover line 8
	// it('returns an empty array when passed an array', () => {
	// 	expect(getFieldsFromTemplate(')).toStrictEqual([]);
	// });
});
